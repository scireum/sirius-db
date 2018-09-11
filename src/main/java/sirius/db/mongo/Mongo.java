/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import com.google.common.collect.Maps;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import sirius.kernel.Startable;
import sirius.kernel.Stoppable;
import sirius.kernel.commons.Explain;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.commons.Watch;
import sirius.kernel.di.PartCollection;
import sirius.kernel.di.std.ConfigValue;
import sirius.kernel.di.std.Parts;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Average;
import sirius.kernel.health.Counter;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.Log;
import sirius.kernel.settings.PortMapper;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Provides a thin layer above Mongo DB with fluent APIs for CRUD operations.
 */
@Register(classes = {Mongo.class, Startable.class, Stoppable.class})
public class Mongo implements Startable, Stoppable {

    private static final String SERVICE_NAME = "mongo";

    @SuppressWarnings("squid:S1192")
    @Explain("Constants have different semantics.")
    public static final Log LOG = Log.get("mongo");

    private static final int MONGO_PORT = 27017;

    private volatile MongoClient mongoClient;

    @ConfigValue("mongo.hosts")
    private String dbHosts;

    @ConfigValue("mongo.db")
    private String dbName;

    @ConfigValue("mongo.logQueryThreshold")
    private Duration longQueryThreshold;
    private long longQueryThresholdMillis = -1;

    @Parts(IndexDescription.class)
    private PartCollection<IndexDescription> indexDescriptions;

    protected boolean temporaryDB;
    protected volatile boolean tracing;
    protected volatile int traceLimit;
    protected Map<String, Tuple<String, String>> traceData = Maps.newConcurrentMap();
    protected Average callDuration = new Average();
    protected Counter numSlowQueries = new Counter();

    /**
     * Determines if access to Mongo DB is configured by checking if a host is given.
     *
     * @return <tt>true</tt> if access to Mongo DB is configured, <tt>false</tt> otherwise
     */
    public boolean isConfigured() {
        return Strings.isFilled(dbHosts);
    }

    /**
     * Provides direct access to the Mongo DB for non-trivial operations.
     *
     * @return an initialized client instance to access Mongo DB.
     */
    public MongoDatabase db() {
        if (mongoClient == null) {
            initializeClient();
        }

        return mongoClient.getDatabase(dbName);
    }

    protected synchronized void initializeClient() {
        if (mongoClient != null) {
            return;
        }

        List<ServerAddress> hosts = Arrays.stream(dbHosts.split(","))
                                          .map(String::trim)
                                          .map(hostname -> PortMapper.mapPort(SERVICE_NAME, hostname, MONGO_PORT))
                                          .map(hostAndPort -> new ServerAddress(hostAndPort.getFirst(),
                                                                                hostAndPort.getSecond()))
                                          .filter(Objects::nonNull)
                                          .collect(Collectors.toList());
        mongoClient = new MongoClient(hosts);

        createIndices(mongoClient.getDatabase(dbName));
    }

    private void createIndices(MongoDatabase db) {
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

    @Override
    public int getPriority() {
        return 75;
    }

    @Override
    public void started() {
        if (isConfigured()) {
            // Force the creation of indices and the initialization of the database connection...
            db();
        }
    }

    @Override
    public void stopped() {
        if (mongoClient != null) {
            mongoClient.close();
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

    /**
     * Returns the query log threshold in millis.
     * <p>
     * If the execution duration of a query is longer than this threshold, it is logged into
     * {@link sirius.db.DB#SLOW_DB_LOG} for further analysis.
     *
     * @return the log thresold for queries in milliseconds
     */
    protected long getLongQueryThresholdMillis() {
        if (longQueryThresholdMillis < 0) {
            longQueryThresholdMillis = longQueryThreshold.toMillis();
        }

        return longQueryThresholdMillis;
    }
}
