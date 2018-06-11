/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.metrics.MetricProvider;
import sirius.kernel.health.metrics.MetricsCollector;

/**
 * Provides metrics for Mongo DB (if configured).
 */
@Register
public class MongoMetricsProvider implements MetricProvider {

    @Part
    private Mongo mongo;

    @Override
    public void gather(MetricsCollector collector) {
        if (mongo.isConfigured()) {
            collector.metric("mongo-calls", "Mongo DB Calls", mongo.callDuration.getCount(), "/min");
            collector.metric("mongo-call-duration", "Mongo DB Call Duration", mongo.callDuration.getAndClear(), "ms");
        }
    }
}
