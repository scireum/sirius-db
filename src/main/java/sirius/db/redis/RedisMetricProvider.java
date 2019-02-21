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
import sirius.kernel.health.metrics.MetricState;
import sirius.kernel.health.metrics.MetricsCollector;

/**
 * Provides metrics for Redis (if configured).
 */
@Register
public class RedisMetricProvider implements MetricProvider {

    @Part
    private Redis redis;

    /**
     * Contains the entry name of the info section under which redis reports the amount of consumed ram
     */
    public static final String INFO_USED_MEMORY = "used_memory";

    /**
     * Contains the entry name of the info section under which redis reports the maximal amount of available ram
     */
    public static final String INFO_MAXMEMORY = "maxmemory";

    @Override
    public void gather(MetricsCollector collector) {
        if (redis.isConfigured()) {
            collector.metric("redis_calls", "redis-calls", "Redis Calls", redis.callDuration.getCount(), "/min");
            collector.metric("redis_call_duration",
                             "redis-call-duration",
                             "Redis Call Duration",
                             redis.callDuration.getAndClear(),
                             "ms");
            collector.metric("redis_memory_usage",
                             "redis-memory-usage",
                             "Redis Memory Usage",
                             Value.of(redis.getInfo().get(INFO_USED_MEMORY)).asLong(0) / 1024d / 1024d,
                             "MB");
            collector.metric("redis_max_memory",
                             "Redis Max Memory",
                             Value.of(redis.getInfo().get(INFO_MAXMEMORY)).asLong(0) / 1024d / 1024d,
                             "MB",
                             MetricState.GRAY);
            collector.metric("redis_messages",
                             "redis-messages",
                             "Redis PubSub Messages",
                             redis.messageDuration.getCount(),
                             "/min");
            collector.metric("redis_message_duration",
                             "redis-message-duration",
                             "Redis PubSub Message Duration",
                             redis.messageDuration.getAndClear(),
                             "ms");
        }
    }
}
