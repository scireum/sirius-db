/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import sirius.db.mixing.Mapping;
import sirius.kernel.commons.Watch;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.Microtiming;

import javax.annotation.Nonnull;

/**
 * Fluent builder to build an update statement.
 */
public class Updater extends QueryBuilder<Updater> {

    private BasicDBObject setObject;
    private BasicDBObject unsetObject;
    private BasicDBObject incObject;
    private BasicDBObject addToSetObject;
    private BasicDBObject pullAllObject;
    private BasicDBObject pullObject;
    private boolean upsert = false;
    private boolean many = false;

    protected Updater(Mongo mongo) {
        super(mongo);
    }

    /**
     * Specifies that multiple documents should be updated.
     * <p>
     * By default only one document is updated.
     *
     * @return the builder itself for fluent method calls
     */
    public Updater many() {
        this.many = true;
        return this;
    }

    /**
     * Specifies that a new document should be created if the update filter does not match anything.
     * <p>
     * By default nothing happens if the update filter does not match anything.
     *
     * @return the builder itself for fluent method calls
     */
    public Updater upsert() {
        this.upsert = true;
        return this;
    }

    /**
     * Sets a field to a new value.
     *
     * @param field the field to update
     * @param value the new value of the field
     * @return the builder itself for fluent method calls
     */
    public Updater set(Mapping field, Object value) {
        return set(field.toString(), value);
    }

    /**
     * Sets a field to a new value.
     *
     * @param field the field to update
     * @param value the new value of the field
     * @return the builder itself for fluent method calls
     */
    public Updater set(String field, Object value) {
        if (setObject == null) {
            setObject = new BasicDBObject();
        }
        setObject.put(field, QueryBuilder.FILTERS.transform(value));

        return this;
    }

    /**
     * Unsets a field.
     *
     * @param field the field to remove
     * @return the builder itself for fluent method calls
     */
    public Updater unset(Mapping field) {
        return unset(field.toString());
    }

    /**
     * Unsets a field.
     *
     * @param field the field to remove
     * @return the builder itself for fluent method calls
     */
    public Updater unset(String field) {
        if (unsetObject == null) {
            unsetObject = new BasicDBObject();
        }
        unsetObject.put(field, "");

        return this;
    }

    /**
     * Sets a field to the given list of values.
     *
     * @param key    the name of the field to set
     * @param values the values to set the field to
     * @return the builder itself for fluent method calls
     */
    public Updater setList(Mapping key, Object... values) {
        return setList(key.toString(), values);
    }

    /**
     * Sets a field to the given list of values.
     *
     * @param key    the name of the field to set
     * @param values the values to set the field to
     * @return the builder itself for fluent method calls
     */
    public Updater setList(String key, Object... values) {
        if (setObject == null) {
            setObject = new BasicDBObject();
        }

        BasicDBList list = new BasicDBList();
        for (Object value : values) {
            list.add(QueryBuilder.FILTERS.transform(value));
        }
        setObject.put(key, list);

        return this;
    }

    /**
     * Increments the given field by the given value.
     *
     * @param field the field to increment
     * @param value the amount by which the field should be incremented
     * @return the builder itself for fluent method calls
     */
    public Updater inc(Mapping field, int value) {
        return inc(field.toString(), value);
    }

    /**
     * Increments the given field by the given value.
     *
     * @param field the field to increment
     * @param value the amount by which the field should be incremented
     * @return the builder itself for fluent method calls
     */
    public Updater inc(String field, int value) {
        if (incObject == null) {
            incObject = new BasicDBObject();
        }
        incObject.put(field, value);

        return this;
    }

    /**
     * Adds the given value to the given set / list.
     *
     * @param field the field containing the set / list
     * @param value the value to add
     * @return the builder itself for fluent method calls
     */
    public Updater addToSet(Mapping field, Object value) {
        return addToSet(field.toString(), value);
    }

    /**
     * Adds the given value to the given set / list.
     *
     * @param field the field containing the set / list
     * @param value the value to add
     * @return the builder itself for fluent method calls
     */
    public Updater addToSet(String field, Object value) {
        if (addToSetObject == null) {
            addToSetObject = new BasicDBObject();
        }
        addToSetObject.put(field, QueryBuilder.FILTERS.transform(value));

        return this;
    }

    /**
     * Removes all occurences of the given values from the list in the given field.
     *
     * @param field  the field containing the list
     * @param values the values to remove
     * @return the builder itself for fluent method calls
     */
    public Updater pullAll(Mapping field, Object... values) {
        return pullAll(field.toString(), values);
    }

    /**
     * Removes all occurences of the given values from the list in the given field.
     *
     * @param field  the field containing the list
     * @param values the values to remove
     * @return the builder itself for fluent method calls
     */
    public Updater pullAll(String field, Object... values) {
        if (pullAllObject == null) {
            pullAllObject = new BasicDBObject();
        }
        BasicDBList valuesToRemove = new BasicDBList();
        for (Object val : values) {
            valuesToRemove.add(QueryBuilder.FILTERS.transform(val));
        }

        pullAllObject.put(field, valuesToRemove);

        return this;
    }

    /**
     * Removes all occurences of the given value from the list in the given field.
     *
     * @param field the field containing the list
     * @param value the value to remove
     * @return the builder itself for fluent method calls
     */
    public Updater pull(Mapping field, Object value) {
        return pull(field.toString(), value);
    }

    /**
     * Removes all occurences of the given value from the list in the given field.
     *
     * @param field the field containing the list
     * @param value the value to remove
     * @return the builder itself for fluent method calls
     */
    public Updater pull(String field, Object value) {
        if (pullObject == null) {
            pullObject = new BasicDBObject();
        }
        pullObject.put(field, QueryBuilder.FILTERS.transform(value));

        return this;
    }

    /**
     * Executes the update on the given collection.
     *
     * @param type the type of entities to update
     * @return the result of the update
     */
    public UpdateResult executeFor(Class<?> type) {
        return executeFor(getRelationName(type));
    }

    /**
     * Executes the update on the given entity.
     *
     * @param entity the entity to filter (by ID) and update
     * @return the result of the update
     */
    public UpdateResult executeFor(MongoEntity entity) {
        where(MongoEntity.ID, entity.getId());
        return executeFor(entity.getDescriptor().getRelationName());
    }

    /**
     * Executes the update on the given collection.
     *
     * @param collection the collection to update
     * @return the result of the update
     */
    public UpdateResult executeFor(String collection) {
        Document updateObject = prepareUpdate(collection);

        Watch w = Watch.start();
        try {
            if (Mongo.LOG.isFINE()) {
                Mongo.LOG.FINE("UPDATE: %s\nFilter: %s\n Update:%s", collection, filterObject, updateObject);
            }
            UpdateOptions updateOptions = new UpdateOptions().upsert(this.upsert);
            if (many) {
                return mongo.db().getCollection(collection).updateMany(filterObject, updateObject, updateOptions);
            } else {
                return mongo.db().getCollection(collection).updateOne(filterObject, updateObject, updateOptions);
            }
        } finally {
            mongo.callDuration.addValue(w.elapsedMillis());
            if (Microtiming.isEnabled()) {
                w.submitMicroTiming("mongo", "UPDATE - " + collection + ": " + filterObject);
            }
            traceIfRequired(collection, w);
        }
    }

    @Nonnull
    protected Document prepareUpdate(String collection) {
        Document updateObject = new Document();
        if (setObject != null) {
            updateObject.put("$set", setObject);
        }
        if (unsetObject != null) {
            updateObject.put("$unset", unsetObject);
        }
        if (incObject != null) {
            updateObject.put("$inc", incObject);
        }
        if (addToSetObject != null) {
            updateObject.put("$addToSet", addToSetObject);
        }
        if (pullAllObject != null) {
            updateObject.put("$pullAll", pullAllObject);
        }
        if (pullObject != null) {
            updateObject.put("$pull", pullObject);
        }

        if (updateObject.isEmpty()) {
            throw Exceptions.handle()
                            .to(Mongo.LOG)
                            .withSystemErrorMessage("Cannot execute an empty update on %s", collection)
                            .handle();
        }
        return updateObject;
    }
}
