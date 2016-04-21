/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.WriteResult;

/**
 * Fluent builder to build a delete statement.
 */
public class Deleter {

    private Mongo mongo;

    private BasicDBObject filterObject = new BasicDBObject();

    protected Deleter(Mongo mongo) {
        this.mongo = mongo;
    }

    /**
     * Adds a condition which determines which documents should be deleted.
     *
     * @param field the name of the field to filter on
     * @param value the value to filter on
     * @return the builder itself for fluent method calls
     */
    public Deleter where(String field, Object value) {
        filterObject.put(field, value);

        return this;
    }

    /**
     * Executes the delete statement on the given collection.
     *
     * @param collection the name of the collection to delete documents from
     * @return the result of the delete operation
     */
    public WriteResult from(String collection) {
        return mongo.db().getCollection(collection).remove(filterObject);
    }
}
