/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.metrics.MetricProvider;
import sirius.kernel.health.metrics.MetricsCollector;

/**
 * Provides metrics for Elasticsearch (if configured).
 */
@Register
public class ElasticMetricsProvider implements MetricProvider {

    @Part
    private Elastic elastic;

    @Override
    public void gather(MetricsCollector collector) {
        if (elastic.isConfigured()) {
            collector.differentialMetric("es-slow-queries",
                                         "es-slow-queries",
                                         "Elasticsearch Slow Queries",
                                         elastic.numSlowQueries.getCount(),
                                         "/min");
            collector.metric("es-calls", "Elasticsearch Calls", elastic.callDuration.getCount(), "/min");
            collector.metric("es-call-duration",
                             "Elasticsearch Call Duration",
                             elastic.callDuration.getAndClear(),
                             "ms");
        }
    }
}
