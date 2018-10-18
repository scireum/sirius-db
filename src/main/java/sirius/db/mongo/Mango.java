/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import org.bson.Document;
import sirius.db.KeyGenerator;
import sirius.db.mixing.BaseMapper;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.OptimisticLockException;
import sirius.db.mixing.Property;
import sirius.db.mixing.annotations.Index;
import sirius.db.mixing.query.constraints.FilterFactory;
import sirius.db.mongo.constraints.MongoConstraint;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;

import java.util.Optional;
import java.util.function.Function;

/**
 * Provides the {@link BaseMapper mapper} used to communicate with <tt>MongoDB</tt>.
 */
@Register(classes = {Mango.class, IndexDescription.class})
public class Mango extends BaseMapper<MongoEntity, MongoConstraint, MongoQuery<?>> implements IndexDescription {

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

    @Part
    private KeyGenerator keyGen;

    @Override
    protected void createEntity(MongoEntity entity, EntityDescriptor ed) throws Exception {
        Inserter insert = mongo.insert();
        String generateId = keyGen.generateId();
        insert.set(MongoEntity.ID, generateId);
        if (ed.isVersioned()) {
            insert.set(VERSION, 1);
        }

        for (Property p : ed.getProperties()) {
            if (!MongoEntity.ID.getName().equals(p.getName())) {
                insert.set(p.getPropertyName(), p.getValueForDatasource(Mango.class, entity));
            }
        }

        insert.into(ed.getRelationName());
        entity.setId(generateId);
        if (ed.isVersioned()) {
            entity.setVersion(1);
        }
    }

    @Override
    protected void updateEntity(MongoEntity entity, boolean force, EntityDescriptor ed) throws Exception {
        Updater updater = mongo.update();
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

        long updatedRows = updater.executeFor(ed.getRelationName()).getModifiedCount();
        enforceUpdate(entity, force, updatedRows);

        if (ed.isVersioned()) {
            entity.setVersion(entity.getVersion() + 1);
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
        Deleter deleter = mongo.delete().where(MongoEntity.ID, entity.getId());
        if (!force && ed.isVersioned()) {
            deleter.where(VERSION, entity.getVersion());
        }

        long numDeleted = deleter.singleFrom(ed.getRelationName()).getDeletedCount();
        if (numDeleted == 0 && !force && ed.isVersioned()) {
            if (mongo.find().where(MongoEntity.ID, entity.getId()).countIn(ed.getRelationName()) > 0) {
                throw new OptimisticLockException();
            }
        }
    }

    @Override
    protected <E extends MongoEntity> Optional<E> findEntity(Object id,
                                                             EntityDescriptor ed,
                                                             Function<String, Value> context) throws Exception {
        return mongo.find()
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
    public void createIndices(MongoDatabase client) {
        mixing.getDesciptors()
              .stream()
              .filter(ed -> MongoEntity.class.isAssignableFrom(ed.getType()))
              .forEach(ed -> createIndices(ed, client));
    }

    private void createIndices(EntityDescriptor ed, MongoDatabase client) {
        ed.getAnnotations(Index.class).forEach(index -> {
            if (index.columnSettings() == null || index.columns().length != index.columnSettings().length) {
                Exceptions.handle()
                          .to(Mongo.LOG)
                          .withSystemErrorMessage(
                                  "Invalid index specification for index %s of %s (%s). We need a columnSetting for each column",
                                  index.name(),
                                  ed.getType().getName(),
                                  ed.getRelationName())
                          .handle();
            } else {
                createIndex(ed, client, index);
            }
        });
    }

    private void createIndex(EntityDescriptor ed, MongoDatabase client, Index index) {
        try {
            Document document = new Document();
            for (int i = 0; i < index.columns().length; i++) {
                Value setting = Value.of(index.columnSettings()[i]);
                document.append(index.columns()[i], setting.isNumeric() ? setting.asInt(1) : setting.asString());
            }

            Mongo.LOG.INFO("Creating MongoDB index %s for: %s...", index.name(), ed.getRelationName());
            client.getCollection(ed.getRelationName()).createIndex(document, new IndexOptions().unique(index.unique()));
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
}
