/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.redis;

import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.metrics.MetricProvider;
import sirius.kernel.health.metrics.MetricsCollector;

/**
 * Provides metrics for Redis (if configured).
 */
@Register
public class RedisMetricProvider implements MetricProvider {

    @Part
    private Redis redis;

    @Override
    public void gather(MetricsCollector collector) {
        if (redis.isConfigured()) {

            collector.differentialMetric("redis-calls",
                                         "redis-calls",
                                         "Redis Calls",
                                         redis.callDuration.getCount(),
                                         "/min");
            collector.metric("redis-call-duration",
                             "Redis Call Duration",
                             redis.callDuration.getAndClearAverage(),
                             "ms");
            collector.metric("redis-memory-usage",
                             "Redis Memory Usage",
                             Value.of(redis.getInfo().get(Redis.INFO_USED_MEMORY)).asLong(0) / 1024d / 1024d,
                             "MB");
            collector.differentialMetric("redis-messages",
                                         "redis-messages",
                                         "Redis Calls",
                                         redis.messageDuration.getCount(),
                                         "/min");
            collector.metric("redis-message-duration",
                             "Redis Message Duration",
                             redis.messageDuration.getAndClearAverage(),
                             "ms");
        }
    }
}
