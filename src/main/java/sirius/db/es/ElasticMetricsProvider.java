/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.metrics.Metric;
import sirius.kernel.health.metrics.MetricProvider;
import sirius.kernel.health.metrics.MetricsCollector;

import java.util.Map;

/**
 * Provides metrics for Elasticsearch (if configured).
 */
@Register(classes = {ElasticMetricsProvider.class, MetricProvider.class})
public class ElasticMetricsProvider implements MetricProvider {

    private static final JsonPointer OLD_GEN_STATS_POINTER = JsonPointer.compile("/jvm/mem/pools/old");

    @Part
    private Elastic elastic;

    @Override
    public void gather(MetricsCollector collector) {
        if (elastic.isConfigured()) {
            collector.differentialMetric("es_slow_queries",
                                         "es-slow-queries",
                                         "Elasticsearch Slow Queries",
                                         elastic.numSlowQueries.getCount(),
                                         "/min");
            collector.metric("es_calls", "es-calls", "Elasticsearch Calls", elastic.callDuration.getCount(), "/min");
            collector.metric("es_call_duration",
                             "es-call-duration",
                             "Elasticsearch Call Duration",
                             elastic.callDuration.getAndClear(),
                             "ms");
            ObjectNode health = elastic.getLowLevelClient().clusterHealth();
            collector.metric("es_unassigned_shards",
                             "es-unassigned-shards",
                             "ES Unassigned Shards",
                             health.path("unassigned_shards").asInt(),
                             null);
            collector.metric("es_memory_pressure",
                             "es-memory-pressure",
                             "Elasticsearch Memory Pressure",
                             getCurrentMaxMemoryPressure(),
                             Metric.UNIT_PERCENT);
        }
    }

    /**
     * Determines the current maximum memory pressure of all ES nodes.
     * <p>
     * The memory pressure is defined as the usage in percent of the old gen memory pool.
     *
     * @return the current maximum memory pressure
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/high-jvm-memory-pressure.html">
     * ElasticSearch reference page for JVM memory pressure</a>
     */
    public int getCurrentMaxMemoryPressure() {
        return elastic.getLowLevelClient()
                      .memoryStats()
                      .path("nodes")
                      .properties()
                      .stream()
                      .map(Map.Entry::getValue)
                      .mapToInt(this::calculateMemoryPressure)
                      .max()
                      .orElse(0);
    }

    private int calculateMemoryPressure(JsonNode memoryStats) {
        JsonNode oldGenStats = memoryStats.at(OLD_GEN_STATS_POINTER);
        int usedInBytes = oldGenStats.path("used_in_bytes").asInt();
        int maxInBytes = oldGenStats.path("max_in_bytes").asInt();

        if (maxInBytes == 0) {
            return 0;
        }

        return (int) (100f * usedInBytes / maxInBytes);
    }
}
