/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import sirius.db.mixing.Mixing;
import sirius.kernel.Sirius;
import sirius.kernel.Startable;
import sirius.kernel.Stoppable;
import sirius.kernel.async.CallContext;
import sirius.kernel.commons.Explain;
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
import sirius.kernel.settings.Extension;
import sirius.kernel.settings.PortMapper;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Provides a thin layer above Mongo DB with fluent APIs for CRUD operations.
 */
@Register(classes = {Mongo.class, Stoppable.class})
public class Mongo implements Startable, Stoppable {

    private static final String SERVICE_NAME = "mongo";

    @SuppressWarnings("squid:S1192")
    @Explain("Constants have different semantics.")
    public static final Log LOG = Log.get("mongo");

    private static final int MONGO_PORT = 27017;

    private final Map<String, Tuple<MongoClient, String>> mongoClients = new ConcurrentHashMap<>();
    private final Map<String, Boolean> mongoClientConfigured = new ConcurrentHashMap<>();

    @ConfigValue("mongo.logQueryThreshold")
    private Duration logQueryThreshold;
    private long logQueryThresholdMillis = -1;

    @Parts(IndexDescription.class)
    private PartCollection<IndexDescription> indexDescriptions;

    protected Average callDuration = new Average();
    protected Average secondaryCallDuration = new Average();
    protected Counter numSlowQueries = new Counter();

    /**
     * Determines if access to Mongo DB is configured by checking if a host is given.
     *
     * @param database the database / configuration to check
     * @return <tt>true</tt> if access to Mongo DB is configured, <tt>false</tt> otherwise
     */
    public boolean isConfigured(String database) {
        return mongoClientConfigured.computeIfAbsent(database,
                                                     db -> Sirius.getSettings()
                                                                 .getExtension("mongo.databases", database)
                                                                 .get("hosts")
                                                                 .isFilled());
    }

    /**
     * Determines if access to default Mongo DB is configured by checking if a host is given.
     *
     * @return <tt>true</tt> if access to Mongo DB is configured, <tt>false</tt> otherwise
     */
    public boolean isConfigured() {
        return isConfigured(Mixing.DEFAULT_REALM);
    }

    /**
     * Provides direct access to the Mongo DB for non-trivial operations.
     *
     * @param database the name of the database configuration to use.
     * @return an initialized client instance to access Mongo DB.
     */
    @Nullable
    public MongoDatabase db(String database) {
        Tuple<MongoClient, String> clientAndDB = mongoClients.computeIfAbsent(database, this::setupClient);
        return clientAndDB.getFirst().getDatabase(clientAndDB.getSecond());
    }

    /**
     * Provides direct access to the default Mongo DB for non-trivial operations.
     *
     * @return an initialized client instance to access Mongo DB.
     */
    public MongoDatabase db() {
        return db(Mixing.DEFAULT_REALM);
    }

    @SuppressWarnings("squid:S2095")
    @Explain("We cannot close the client here as it is part of the return value.")
    protected synchronized Tuple<MongoClient, String> setupClient(String database) {
        Extension config = Sirius.getSettings().getExtension("mongo.databases", database);
        String connectionString = Arrays.stream(config.get("hosts").asString().split(","))
                                        .map(String::trim)
                                        .map(hostname -> PortMapper.mapPort(SERVICE_NAME, hostname, MONGO_PORT))
                                        .map(hostAndPort -> hostAndPort.getFirst() + ":" + hostAndPort.getSecond())
                                        .collect(Collectors.joining(","));
        MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder()
                                                                         .applyConnectionString(new ConnectionString(
                                                                                 "mongodb://" + connectionString))
                                                                         .applicationName(CallContext.getNodeName());
        MongoCredential credentials = determineCredentials(config);
        if (credentials != null) {
            settingsBuilder.credential(credentials);
        }

        MongoClient mongoClient = MongoClients.create(settingsBuilder.build());
        createIndices(database, mongoClient.getDatabase(config.get("db").asString()));
        return Tuple.create(mongoClient, config.get("db").asString());
    }

    private MongoCredential determineCredentials(Extension config) {
        if (config.get("user").isEmptyString() || config.get("password").isEmptyString()) {
            return null;
        }

        return MongoCredential.createCredential(config.get("user").asString(),
                                                config.get("userDatabase").asString(config.get("db").asString()),
                                                config.get("password").asString().toCharArray());
    }

    private void createIndices(String database, MongoDatabase db) {
        for (IndexDescription idx : indexDescriptions) {
            Watch w = Watch.start();
            try {
                LOG.INFO("Creating indices in Mongo DB: %s", idx.getClass().getName());
                idx.createIndices(database, db);
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
        mongoClients.values().stream().map(Tuple::getFirst).forEach(client -> {
            try {
                client.close();
            } catch (Exception e) {
                LOG.WARN(e);
            }
        });

        mongoClients.clear();
    }

    /**
     * Returns a fluent query builder to insert a document into the database.
     *
     * @param database the name of the database configuration to use
     * @return a query builder to create an insert statement
     */
    public Inserter insert(String database) {
        return new Inserter(this, database);
    }

    /**
     * Returns a fluent query builder to insert a document into the default database.
     *
     * @return a query builder to create an insert statement
     */
    public Inserter insert() {
        return insert(Mixing.DEFAULT_REALM);
    }

    /**
     * Returns a fluent query builder to find one or more documents in the database
     *
     * @param database       the name of the database configuration to use
     * @param readPreference the read preference to enforce. Note that one should most probably rather use
     *                       {@link #find(String)} or {@link #findInSecondary(String)} to enforce a proper read
     *                       reference. However, as this only permits to use <tt>PRIMARY</tt> or <tt>NEAREST</tt>, we
     *                       still permit to specify a custom preference here. Note that <tt>null</tt> and
     *                       <tt>PRIMARY</tt> are treated indentically.
     * @return a query builder to create a find statement
     */
    public Finder find(String database, @Nullable ReadPreference readPreference) {
        return new Finder(this, database, readPreference);
    }

    /**
     * Returns a fluent query builder to find one or more documents in the database
     *
     * @param database the name of the database configuration to use
     * @return a query builder to create a find statement
     */
    public Finder find(String database) {
        return find(database, null);
    }

    /**
     * Returns a fluent query builder to find one or more documents in the secondary database.
     * <p>
     * This provides an essential boost in performance, as all nodes of a MongoDB cluster are utilized. However, this
     * may return stale data if a secondary lags behind. Therefore this data must not be stored back in the primary
     * database. This should rather only be used to serve web requests or other queries where occasional stale date
     * does no harm.
     * <p>
     * Also, this should NOT be used to fill any cache as this might poison the cache with already stale data.
     *
     * @param database the name of the database configuration to use
     * @return a query builder to create a find statement
     */
    public Finder findInSecondary(String database) {
        return find(database, ReadPreference.nearest());
    }

    /**
     * Returns a fluent query builder to find one or more documents in the default database
     *
     * @return a query builder to create a find statement
     */
    public Finder find() {
        return find(Mixing.DEFAULT_REALM);
    }

    /**
     * Returns a fluent query builder to find one or more documents in the default database
     * <p>
     * This provides an essential boost in performance, as all nodes of a MongoDB cluster are utilized. However, this
     * may return stale data if a secondary lags behind. Therefore this data must not be stored back in the primary
     * database. This should rather only be used to serve web requests or other queries where occasional stale date
     * does no harm.
     * <p>
     * Also, this should NOT be used to fill any cache as this might poison the cache with already stale data.
     *
     * @return a query builder to create a find statement
     */
    public Finder findInSecondary() {
        return findInSecondary(Mixing.DEFAULT_REALM);
    }

    /**
     * Returns a fluent query builder to update one or more documents in the database.
     *
     * @param database the name of the database configuration to use.
     * @return a query builder to create an update statement
     */
    public Updater update(String database) {
        return new Updater(this, database);
    }

    /**
     * Returns a fluent query builder to update one or more documents in the default database.
     *
     * @return a query builder to create an update statement
     */
    public Updater update() {
        return update(Mixing.DEFAULT_REALM);
    }

    /**
     * Returns a fluent query builder to delete one or more documents in the database.
     *
     * @param database the name of the database configuration to use.
     * @return a query builder to create a delete statement
     */
    public Deleter delete(String database) {
        return new Deleter(this, database);
    }

    /**
     * Returns a fluent query builder to delete one or more documents in the default database.
     *
     * @return a query builder to create a delete statement
     */
    public Deleter delete() {
        return delete(Mixing.DEFAULT_REALM);
    }

    /**
     * Returns the query log threshold in millis.
     * <p>
     * If the execution duration of a query is longer than this threshold, it is logged into
     * {@link sirius.db.DB#SLOW_DB_LOG} for further analysis.
     *
     * @return the log thresold for queries in milliseconds
     */
    protected long getLogQueryThresholdMillis() {
        if (logQueryThresholdMillis < 0) {
            logQueryThresholdMillis = logQueryThreshold.toMillis();
        }

        return logQueryThresholdMillis;
    }
}
