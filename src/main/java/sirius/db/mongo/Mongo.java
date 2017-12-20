/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import com.google.common.collect.Maps;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.commons.Watch;
import sirius.kernel.di.PartCollection;
import sirius.kernel.di.std.ConfigValue;
import sirius.kernel.di.std.Parts;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Average;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.Log;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Provides a thin layer above Mongo DB with fluent APIs for CRUD operations.
 */
@Register(classes = Mongo.class)
public class Mongo {

    public static final Log LOG = Log.get("mongo");

    private volatile MongoClient mongoClient;

    @ConfigValue("mongo.host")
    private String dbHost;

    @ConfigValue("mongo.hosts")
    private List<String> dbHosts;

    @ConfigValue("mongo.db")
    private String dbName;

    @Parts(IndexDescription.class)
    private PartCollection<IndexDescription> indexDescriptions;

    protected volatile boolean tracing;
    protected volatile int traceLimit;
    protected Map<String, Tuple<String, String>> traceData = Maps.newConcurrentMap();
    protected Average callDuration = new Average();

    /**
     * Determines if access to Mongo DB is configured by checking if a host is given.
     *
     * @return <tt>true</tt> if access to Mongo DB is configured, <tt>false</tt> otherwise
     */
    public boolean isConfigured() {
        return Strings.isFilled(dbHost) || !dbHosts.isEmpty();
    }

    /**
     * Provides direct access to the Mongo DB for non-trivial operations.
     *
     * @return a database object to access Mongo DB.
     */
    public DB db() {
        if (mongoClient == null) {
            initializeClient();
        }

        return mongoClient.getDB(dbName);
    }

    /**
     * Provides direct access to the underlying Mongo DB client for non-trivial operations.
     *
     * @return an initialized client instance to access Mongo DB.
     */
    public MongoClient client() {
        if (mongoClient == null) {
            initializeClient();
        }

        return mongoClient;
    }

    protected synchronized void initializeClient() {
        if (mongoClient != null) {
            return;
        }

        if (dbHosts.isEmpty()) {
            mongoClient = new MongoClient(dbHost);
        } else {
            mongoClient = new MongoClient(dbHosts.stream()
                                                 .map(ServerAddress::new)
                                                 .filter(Objects::nonNull)
                                                 .collect(Collectors.toList()));
        }

        createIndices(mongoClient.getDB(dbName));
    }

    private void createIndices(DB db) {
        for (IndexDescription idx : indexDescriptions) {
            Watch w = Watch.start();
            try {
                LOG.INFO("Creating indices in Mongo DB: %s", idx.getClass().getName());
                idx.createIndices(db);
                LOG.INFO("Completed indices for: %s (%s)", idx.getClass().getName(), w.duration());
            } catch (Exception t) {
                Exceptions.handle()
                          .to(LOG)
                          .error(t)
                          .withSystemErrorMessage("Error while creating indices for '%s': %s (%s)",
                                                  idx.getClass().getName())
                          .handle();
            }
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
