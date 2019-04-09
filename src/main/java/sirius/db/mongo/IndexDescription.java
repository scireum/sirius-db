/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import com.mongodb.client.MongoDatabase;

/**
 * Used to setup indices in Mongo DB on system startup.
 * <p>
 * Once the first operation is triggered against Mongo DB, the framework finds all classes implementing this interface.
 * To be discovered, the class has to wear a {@link sirius.kernel.di.std.Register} annotation.
 */
public interface IndexDescription {

    /**
     * Invoked once the Mongo DB is first accessed and permits to create required indices.
     *
     * @param database the name of the database (in the configuration) for which the indices are created
     * @param client   can be used to create indices in the Mongo DB
     */
    void createIndices(String database, MongoDatabase client);
}
