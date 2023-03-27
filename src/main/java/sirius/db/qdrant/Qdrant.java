/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.qdrant;

import sirius.kernel.commons.Strings;
import sirius.kernel.di.std.ConfigValue;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Average;
import sirius.kernel.health.Log;
import sirius.kernel.health.metrics.MetricProvider;
import sirius.kernel.health.metrics.MetricsCollector;

/**
 * Provides access to a qdrant vector database.
 * <p>
 * Note that we currently only support accessing a single instance (configured via <tt>qdrant.default.host</tt>).
 * However, the access to the database is already encapsulated in {@link QdrantDatabase} and thus adding support
 * for multiple instances can be easily added later on, if needed.
 */
@Register(classes = {Qdrant.class, MetricProvider.class})
public class Qdrant implements MetricProvider {

    /**
     * Provides a common logger for all qdrant related messages.
     */
    public static final Log LOG = Log.get("qdrant");

    protected static final Average callDuration = new Average();

    @ConfigValue("qdrant.default.host")
    private String host;

    private QdrantDatabase db;

    /**
     * Determines if the qdrant database is configured.
     *
     * @return <tt>true</tt> if the database is configured, <tt>false</tt> otherwise
     */
    public boolean isConfigured() {
        return Strings.isFilled(host);
    }

    /**
     * Provides access to the qdrant database.
     *
     * @return the qdrant database being configured in the system config
     */
    public QdrantDatabase db() {
        if (db == null) {
            db = new QdrantDatabase(host);
        }

        return db;
    }

    @Override
    public void gather(MetricsCollector collector) {
        if (isConfigured()) {
            collector.metric("qdrant_calls", "qdrant-calls", "qdrant Calls", callDuration.getCount(), "/min");
            collector.metric("qdrant_call_duration",
                             "qdrant-call-duration",
                             "qdrant Call Duration",
                             callDuration.getAndClear(),
                             "ms");
        }
    }
}
