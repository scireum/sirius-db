/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import sirius.kernel.di.std.ConfigValue;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.Log;

import java.net.UnknownHostException;

/**
 * Provides a thin layer above Mongo DB with fluent APIs for CRUD operations.
 */
@Register(classes = Mongo.class)
public class Mongo {

    public static final Log LOG = Log.get("mongo");

    private MongoClient mongoClient;

    @ConfigValue("mongo.host")
    private String dbHost;

    @ConfigValue("mongo.db")
    private String dbName;

    /**
     * Provides direct access to the Mongo DB for non-trivial operations.
     *
     * @return an initialized client instance to access Mongo DB.
     */
    public DB db() {
        try {
            if (mongoClient == null) {
                mongoClient = new MongoClient(dbHost);
            }

            return mongoClient.getDB(dbName);
        } catch (UnknownHostException e) {
            throw Exceptions.handle(e);
        }
    }

    /**
     * Returns a fluent query builder to insert a document into the database
     *
     * @return a query builder to create an insert statement
     */
    public Inserter insert() {
        return new Inserter(this);
    }

    /**
     * Returns a fluent query builder to find one or more documents in the database
     *
     * @return a query builder to create a find statement
     */
    public Finder find() {
        return new Finder(this);
    }

    /**
     * Returns a fluent query builder to update one or more documents in the database
     *
     * @return a query builder to create an update statement
     */
    public Updater update() {
        return new Updater(this);
    }

    /**
     * Returns a fluent query builder to delete one or more documents in the database
     *
     * @return a query builder to create a delete statement
     */
    public Deleter delete() {
        return new Deleter(this);
    }
}
