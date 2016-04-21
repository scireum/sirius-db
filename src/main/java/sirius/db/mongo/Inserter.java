/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import com.mongodb.BasicDBObject;

/**
 * Fluent builder to build an insert statement.
 */
public class Inserter {

    private BasicDBObject obj;
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
    public Inserter set(String key, Object value) {

        obj.put(key, value);
        return this;
    }

    /**
     * Executes the insert statement into the given collection.
     *
     * @param collection the collection to insert the document into
     * @return the inserted document
     */
    public Document into(String collection) {
        mongo.db().getCollection(collection).insert(obj);
        return new Document(obj);
    }
}
