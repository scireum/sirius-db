/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import com.mongodb.WriteResult;
import sirius.kernel.commons.Watch;
import sirius.kernel.health.Microtiming;

/**
 * Fluent builder to build a delete statement.
 */
public class Deleter extends QueryBuilder<Deleter> {

    protected Deleter(Mongo mongo) {
        super(mongo);
    }

    /**
     * Executes the delete statement on the given collection.
     *
     * @param collection the name of the collection to delete documents from
     * @return the result of the delete operation
     */
    public WriteResult from(String collection) {
        Watch w = Watch.start();
        try {
            return mongo.db().getCollection(collection).remove(filterObject);
        } finally {
            mongo.callDuration.addValue(w.elapsedMillis());
            if (Microtiming.isEnabled()) {
                w.submitMicroTiming("mongo", "DELETE - " + collection + ": " + filterObject);
            }
            traceIfRequired(collection, w);
        }
    }
}
