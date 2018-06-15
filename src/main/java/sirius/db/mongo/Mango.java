/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import sirius.db.KeyGenerator;
import sirius.db.mixing.BaseMapper;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.OptimisticLockException;
import sirius.db.mixing.Property;
import sirius.db.mixing.annotations.Index;
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
public class Mango extends BaseMapper<MongoEntity, MongoQuery<?>> implements IndexDescription {

    /**
     * Defines the value used to desclare an index as sorted in ascending order.
     */
    public static final String INDEX_ASCENDING = "1";

    /**
     * Defines the value used to desclare an index as sorted in descending order.
     */
    public static final String INDEX_DESCENDING = "-1";

    @Part
    private Mongo mongo;

    @Part
    private KeyGenerator keyGen;

    @Override
    protected void createEnity(MongoEntity entity, EntityDescriptor ed) throws Exception {
        Inserter insert = mongo.insert();
        String generateId = keyGen.generateId();
        insert.set(MongoEntity.ID, generateId);
        for (Property p : ed.getProperties()) {
            if (!MongoEntity.ID.getName().equals(p.getName())) {
                insert.set(p.getPropertyName(), p.getValueForDatasource(entity));
            }
        }

        insert.into(ed.getRelationName());
        entity.setId(generateId);
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
                if (VersionedEntity.VERSION.getName().equals(p.getName())) {
                    throw new IllegalStateException("The version column of an entity must not be modified manually!");
                }

                updater.set(p.getPropertyName(), p.getValueForDatasource(entity));
                changed = true;
            }
        }

        if (!changed) {
            return;
        }

        updater.where(MongoEntity.ID, entity.getId());
        boolean versioned = entity instanceof VersionedEntity;
        if (versioned) {
            updater.set(VersionedEntity.VERSION, ((VersionedEntity) entity).getVersion() + 1);
            if (!force) {
                updater.where(VersionedEntity.VERSION, ((VersionedEntity) entity).getVersion());
            }
        }

        long updatedRows = updater.executeFor(ed.getRelationName()).getModifiedCount();
        enforceUpdate(entity, force, updatedRows);

        if (versioned) {
            ((VersionedEntity) entity).setVersion(((VersionedEntity) entity).getVersion() + 1);
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
        boolean versioned = !force && entity instanceof VersionedEntity;
        if (versioned) {
            deleter.where(VersionedEntity.VERSION, ((VersionedEntity) entity).getVersion());
        }

        long numDeleted = deleter.singleFrom(ed.getRelationName()).getDeletedCount();
        if (numDeleted == 0 && versioned) {
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
            return (E) ed.make(null, doc::get);
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

    public String getCollection(Class<? extends MongoEntity> type) {
        return mixing.getDescriptor(type).getRelationName();
    }

    @Override
    public void createIndices(MongoDatabase client) {
        mixing.getDesciptors()
              .stream()
              .filter(ed -> MongoEntity.class.isAssignableFrom(ed.getType()))
              .forEach(ed -> createIndices(ed, ed.getType(), client));
    }

    private void createIndices(EntityDescriptor ed, Class<?> type, MongoDatabase client) {
        for (Index index : type.getAnnotationsByType(Index.class)) {
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
        }

        if (type.getSuperclass() != null && !Object.class.equals(type.getSuperclass())) {
            createIndices(ed, type.getSuperclass(), client);
        }
    }

    private void createIndex(EntityDescriptor ed, MongoDatabase client, Index index) {
        try {
            Document document = new Document();
            for (int i = 0; i < index.columns().length; i++) {
                Value setting = Value.of(index.columnSettings()[i]);
                document.append(index.columns()[i], setting.isNumeric() ? setting.asInt(1) : setting.asString());
            }

            Mongo.LOG.INFO("Creating MongoDB index %s for: %s...", index.name(), ed.getRelationName());
            client.getCollection(ed.getRelationName()).createIndex(document);
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
