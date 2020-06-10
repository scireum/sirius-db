/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import org.bson.Document;
import sirius.db.mixing.BaseMapper;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.IntegrityConstraintFailedException;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.OptimisticLockException;
import sirius.db.mixing.Property;
import sirius.db.mixing.annotations.Index;
import sirius.db.mixing.query.constraints.FilterFactory;
import sirius.db.mongo.constraints.MongoConstraint;
import sirius.kernel.Startable;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;

import java.util.HashSet;
import java.util.IntSummaryStatistics;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Provides the {@link BaseMapper mapper} used to communicate with <tt>MongoDB</tt>.
 */
@Register(classes = {Mango.class, Startable.class})
public class Mango extends BaseMapper<MongoEntity, MongoConstraint, MongoQuery<?>> implements Startable {

    /**
     * Defines the name of the internal ID field in MongoDB
     */
    public static final String ID_FIELD = "_id";

    /**
     * Defines the value used to desclare an index as sorted in ascending order.
     */
    public static final String INDEX_ASCENDING = "1";

    /**
     * Defines the value used to desclare an index as sorted in descending order.
     */
    public static final String INDEX_DESCENDING = "-1";

    /**
     * Defines the value used to desclare create a fulltext index for the given column.
     */
    public static final String INDEX_AS_FULLTEXT = "text";

    @Part
    private Mongo mongo;

    @Override
    protected void createEntity(MongoEntity entity, EntityDescriptor ed) throws Exception {
        Inserter insert = mongo.insert();
        String generatedId = entity.generateId();
        insert.set(MongoEntity.ID, generatedId);
        if (ed.isVersioned()) {
            insert.set(VERSION, 1);
        }

        for (Property p : ed.getProperties()) {
            if (!MongoEntity.ID.getName().equals(p.getName())) {
                insert.set(p.getPropertyName(), p.getValueForDatasource(Mango.class, entity));
            }
        }

        try {
            insert.into(ed.getRelationName());
            entity.setId(generatedId);
            if (ed.isVersioned()) {
                entity.setVersion(1);
            }
        } catch (MongoWriteException e) {
            if (e.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
                throw new IntegrityConstraintFailedException(e);
            } else {
                throw e;
            }
        }
    }

    @Override
    protected void updateEntity(MongoEntity entity, boolean force, EntityDescriptor ed) throws Exception {
        Updater updater = mongo.update(ed.getRealm());
        boolean changed = false;
        for (Property p : ed.getProperties()) {
            if (ed.isChanged(entity, p)) {
                if (MongoEntity.ID.getName().equals(p.getName())) {
                    throw new IllegalStateException("The id column of an entity must not be modified manually!");
                }

                updater.set(p.getPropertyName(), p.getValueForDatasource(Mango.class, entity));
                changed = true;
            }
        }

        if (!changed) {
            return;
        }

        updater.where(MongoEntity.ID, entity.getId());
        if (ed.isVersioned()) {
            updater.set(VERSION, entity.getVersion() + 1);
            if (!force) {
                updater.where(VERSION, entity.getVersion());
            }
        }

        try {
            long updatedRows = updater.executeFor(ed.getRelationName()).getModifiedCount();
            enforceUpdate(entity, force, updatedRows);

            if (ed.isVersioned()) {
                entity.setVersion(entity.getVersion() + 1);
            }
        } catch (MongoWriteException e) {
            if (e.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
                throw new IntegrityConstraintFailedException(e);
            } else {
                throw e;
            }
        }
    }

    private <E extends MongoEntity> void enforceUpdate(E entity, boolean force, long updatedRows)
            throws OptimisticLockException {
        if (force || updatedRows > 0) {
            return;
        }
        if (find(entity.getClass(), entity.getId()).isPresent()) {
            throw new OptimisticLockException();
        } else {
            throw Exceptions.handle()
                            .to(Mongo.LOG)
                            .withSystemErrorMessage(
                                    "The entity %s (%s) cannot be updated as it does not exist in the database!",
                                    entity,
                                    entity.getId())
                            .handle();
        }
    }

    @Override
    protected void deleteEntity(MongoEntity entity, boolean force, EntityDescriptor ed) throws Exception {
        Deleter deleter = mongo.delete(ed.getRealm()).where(MongoEntity.ID, entity.getId());
        if (!force && ed.isVersioned()) {
            deleter.where(VERSION, entity.getVersion());
        }

        long numDeleted = deleter.singleFrom(ed.getRelationName()).getDeletedCount();
        if (numDeleted == 0
            && !force
            && ed.isVersioned()
            && mongo.find().where(MongoEntity.ID, entity.getId()).countIn(ed.getRelationName()) > 0) {
            throw new OptimisticLockException();
        }
    }

    @Override
    protected <E extends MongoEntity> Optional<E> findEntity(Object id,
                                                             EntityDescriptor ed,
                                                             Function<String, Value> context) throws Exception {
        return mongo.find(ed.getRealm())
                    .where(MongoEntity.ID, id.toString())
                    .singleIn(ed.getRelationName())
                    .map(doc -> make(ed, doc));
    }

    /**
     * Creates a new entity for the given descriptor based on the given doc.
     *
     * @param ed  the descriptor of the entity to create
     * @param doc the document to read the values from
     * @param <E> the effective type of the generated entity
     * @return the generated entity
     */
    @SuppressWarnings("unchecked")
    public static <E extends MongoEntity> E make(EntityDescriptor ed, Doc doc) {
        try {
            E result = (E) ed.make(Mango.class,
                                   null,
                                   key -> doc.getUnderlyingObject().containsKey(key) ? doc.get(key) : null);
            if (ed.isVersioned()) {
                result.setVersion(doc.get(VERSION).asInt(0));
            }
            return result;
        } catch (Exception e) {
            throw Exceptions.handle(Mongo.LOG, e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <E extends MongoEntity> Optional<E> findEntity(E entity) {
        return find((Class<E>) entity.getClass(), entity.getId());
    }

    @Override
    public <E extends MongoEntity> MongoQuery<E> select(Class<E> type) {
        return new MongoQuery<>(mixing.getDescriptor(type));
    }

    @Override
    public FilterFactory<MongoConstraint> filters() {
        return QueryBuilder.FILTERS;
    }

    /**
     * Returns the collection name for the given entity type.
     *
     * @param type the type to get the collection for
     * @return the name of the collection used to store the given entity type
     */
    public String getCollection(Class<? extends MongoEntity> type) {
        return mixing.getDescriptor(type).getRelationName();
    }

    @Override
    public int getPriority() {
        return 75;
    }

    @Override
    public void started() {
        if (!mixing.shouldExecuteSafeSchemaChanges()) {
            Mongo.LOG.INFO("Skipping index checks on this node...");
            return;
        }

        IntSummaryStatistics createdIndices = mixing.getDescriptors()
                                                    .stream()
                                                    .filter(ed -> MongoEntity.class.isAssignableFrom(ed.getType()))
                                                    .mapToInt(this::createIndices)
                                                    .summaryStatistics();
        Mongo.LOG.INFO("Initialized %s indices for %s collections", createdIndices.getSum(), createdIndices.getCount());
    }

    private int createIndices(EntityDescriptor entityDescriptor) {
        String database = entityDescriptor.getRealm();
        if (!mongo.isConfigured(database)) {
            Mongo.LOG.WARN("Skipping MongoDB indices for: %s as no configuration for database %s is present...",
                           entityDescriptor.getRelationName(),
                           database);
            return 0;
        }

        Set<String> seenIndices = new HashSet<>();
        entityDescriptor.getAnnotations(Index.class)
                        .filter(index -> deduplicateByName(index, seenIndices))
                        .filter(this::skipParentIndexSuppressions)
                        .filter(index -> checkColumnSettings(index, entityDescriptor))
                        .forEach(index -> createIndex(entityDescriptor, mongo.db(database), index));

        return seenIndices.size();
    }

    /**
     * Skips indices which have already been defined by a more concrete class.
     * <p>
     * This permits entities to overwrite indices defined by their parent entities.
     *
     * @param index       the index to check
     * @param seenIndices the set of seen index names
     * @return <tt>true</tt> if the name has to been seen yet, <tt>false</tt> otherwise
     */
    private boolean deduplicateByName(Index index, Set<String> seenIndices) {
        return seenIndices.add(index.name());
    }

    /**
     * Filters indices without any columns.
     * <p>
     * Such indices are used to suppress an index defined by a parent entity.
     *
     * @param index the index to check
     * @return <tt>true</tt> if this is a valid index, <tt>false</tt> if this is a suppression index without columns
     */
    private boolean skipParentIndexSuppressions(Index index) {
        return index.columns().length > 0;
    }

    /**
     * Ensures that there is a {@link Index#columnSettings() column setting} for each {@link Index#columns() column}
     * defined by the index.
     *
     * @param index            the index to check
     * @param entityDescriptor the entitiy descriptor used to generate proper error messages
     * @return <tt>true</tt> if the index is properly populated, <tt>false</tt> otherwise
     */
    private boolean checkColumnSettings(Index index, EntityDescriptor entityDescriptor) {
        if (index.columnSettings() != null && index.columns().length == index.columnSettings().length) {
            return true;
        }

        Exceptions.handle()
                  .to(Mongo.LOG)
                  .withSystemErrorMessage(
                          "Invalid index specification for index %s of %s (%s). We need a columnSetting for each column",
                          index.name(),
                          entityDescriptor.getType().getName(),
                          entityDescriptor.getRelationName())
                  .handle();
        return false;
    }

    private void createIndex(EntityDescriptor ed, MongoDatabase client, Index index) {
        try {
            boolean textColumnSeen = false;
            Document document = new Document();
            for (int i = 0; i < index.columns().length; i++) {
                Value setting = Value.of(index.columnSettings()[i]);
                document.append(index.columns()[i], setting.isNumeric() ? setting.asInt(1) : setting.asString());
                textColumnSeen |= Mango.INDEX_AS_FULLTEXT.equals(setting.getString());
            }

            IndexOptions indexOptions = new IndexOptions().name(index.name()).unique(index.unique());
            if (!textColumnSeen) {
                indexOptions.collation(mongo.determineCollation());
            }

            Mongo.LOG.FINE("Creating MongoDB index %s for: %s...", index.name(), ed.getRelationName());
            client.getCollection(ed.getRelationName()).createIndex(document, indexOptions);
        } catch (Exception e) {
            Exceptions.handle()
                      .error(e)
                      .to(Mongo.LOG)
                      .withSystemErrorMessage("Failed to create index %s of %s (%s) - %s (%s)",
                                              index.name(),
                                              ed.getType().getName(),
                                              ed.getRelationName())
                      .handle();
        }
    }

    @Override
    public Value fetchField(Class<? extends MongoEntity> type, Object id, Mapping field) throws Exception {
        if (Strings.isEmpty(id)) {
            return Value.EMPTY;
        }

        EntityDescriptor descriptor = mixing.getDescriptor(type);
        return mongo.find(descriptor.getRealm())
                    .selectFields(field)
                    .where(MongoEntity.ID, id)
                    .singleIn(descriptor.getRelationName())
                    .map(doc -> doc.get(field))
                    .orElse(Value.EMPTY);
    }

    @Override
    protected int determineRetryTimeoutFactor() {
        return 50;
    }
}
