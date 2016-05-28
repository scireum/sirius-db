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
import com.mongodb.WriteResult;
import sirius.kernel.commons.Watch;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.Microtiming;

/**
 * Fluent builder to build an update statement.
 */
public class Updater extends QueryBuilder<Updater> {

    private BasicDBObject setObject;
    private BasicDBObject incObject;
    private BasicDBObject addToSetObject;
    private BasicDBObject pullAllObject;
    private BasicDBObject pullObject;
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
        setObject.put(field, transformValue(value));

        return this;
    }

    /**
     * Sets a field to the given list of values.
     *
     * @param key    the name of the field to set
     * @param values the values to set the field to
     * @return the builder itself for fluent method calls
     */
    public Updater setList(String key, Object... values) {
        BasicDBList list = new BasicDBList();
        for (Object value : values) {
            list.add(QueryBuilder.transformValue(value));
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
    public Updater addToSet(String field, Object value) {
        if (addToSetObject == null) {
            addToSetObject = new BasicDBObject();
        }
        addToSetObject.put(field, transformValue(value));

        return this;
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
            valuesToRemove.add(transformValue(val));
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
    public Updater pull(String field, Object value) {
        if (pullObject == null) {
            pullObject = new BasicDBObject();
        }
        pullObject.put(field, transformValue(value));

        return this;
    }

    /**
     * Executes the update on the given collection.
     *
     * @param collection the collection to update
     * @return the result of the update
     */
    public WriteResult executeFor(String collection) {
        BasicDBObject updateObject = new BasicDBObject();
        if (setObject != null) {
            updateObject.put("$set", setObject);
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

        Watch w = Watch.start();
        try {
            if (many) {
                return mongo.db().getCollection(collection).updateMulti(filterObject, updateObject);
            } else {
                return mongo.db().getCollection(collection).update(filterObject, updateObject);
            }
        } finally {
            mongo.callDuration.addValue(w.elapsedMillis());
            if (Microtiming.isEnabled()) {
                w.submitMicroTiming("mongo", "UPDATE - " + collection + ": " + filterObject);
            }
            traceIfRequired(collection, w);
        }
    }
}
