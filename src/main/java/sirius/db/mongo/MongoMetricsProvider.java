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
            collector.metric("mongo_calls", "mongo-calls", "Mongo DB Calls", mongo.callDuration.getCount(), "/min");
            collector.metric("mongo_call_duration",
                             "mongo-call-duration",
                             "Mongo DB Call Duration",
                             mongo.callDuration.getAndClear(),
                             "ms");

            collector.metric("mongo_secondary_calls",
                             "mongo-secondary-calls",
                             "Mongo DB Secondary Calls",
                             mongo.secondaryCallDuration.getCount(),
                             "/min");
            collector.metric("mongo_secondary_call_duration",
                             "mongo-secondary-call-duration",
                             "Mongo DB Secondary Call Duration",
                             mongo.secondaryCallDuration.getAndClear(),
                             "ms");

            collector.differentialMetric("mongo_slow_queries",
                                         "mongo-slow-queries",
                                         "MongoDB Slow Queries",
                                         mongo.numSlowQueries.getCount(),
                                         "/min");
        }
    }
}
