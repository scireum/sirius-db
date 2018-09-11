/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import com.mongodb.BasicDBList;
import org.bson.Document;
import sirius.db.mixing.Mapping;
import sirius.kernel.commons.Watch;
import sirius.kernel.health.Microtiming;

/**
 * Fluent builder to build an insert statement.
 */
public class Inserter {

    private Document obj = new Document();
    private Mongo mongo;

    protected Inserter(Mongo mongo) {
        this.mongo = mongo;
    }

    /**
     * Sets a field to the given value.
     *
     * @param key   the name of the field to set
     * @param value the value to set the field to
     * @return the builder itself for fluent method calls
     */
    public Inserter set(Mapping key, Object value) {
        return set(key.toString(), value);
    }

    /**
     * Sets a field to the given value.
     *
     * @param key   the name of the field to set
     * @param value the value to set the field to
     * @return the builder itself for fluent method calls
     */
    public Inserter set(String key, Object value) {
        obj.put(key, QueryBuilder.FILTERS.transform(value));
        return this;
    }

    /**
     * Sets a field to the given list of values.
     *
     * @param key    the name of the field to set
     * @param values the values to set the field to
     * @return the builder itself for fluent method calls
     */
    public Inserter setList(Mapping key, Object... values) {
        return setList(key.toString(), values);
    }

    /**
     * Sets a field to the given list of values.
     *
     * @param key    the name of the field to set
     * @param values the values to set the field to
     * @return the builder itself for fluent method calls
     */
    public Inserter setList(String key, Object... values) {
        BasicDBList list = new BasicDBList();
        for (Object value : values) {
            list.add(QueryBuilder.FILTERS.transform(value));
        }
        obj.put(key, list);
        return this;
    }

    /**
     * Executes the insert statement into the given collection.
     *
     * @param type the type to insert into
     * @return the inserted document
     */
    public Doc into(Class<?> type) {
        return into(QueryBuilder.getRelationName(type));
    }

    /**
     * Executes the insert statement into the given collection.
     *
     * @param collection the collection to insert the document into
     * @return the inserted document
     */
    public Doc into(String collection) {
        if (Mongo.LOG.isFINE()) {
            Mongo.LOG.FINE("INSERT: %s\nObject: %s", collection, obj);
        }

        Watch w = Watch.start();

        mongo.db().getCollection(collection).insertOne(obj);
        mongo.callDuration.addValue(w.elapsedMillis());
        if (Microtiming.isEnabled()) {
            w.submitMicroTiming("mongo", "INSERT - " + collection + ": " + obj);
        }
        return new Doc(obj);
    }
}
