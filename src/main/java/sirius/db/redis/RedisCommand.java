/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.redis;

import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.console.Command;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * Reports all available statistics for Redis.
 */
@Register
public class RedisCommand implements Command {

    @Part
    private Redis redis;

    @Override
    public void execute(Output output, String... params) throws Exception {
        if (!redis.isConfigured()) {
            output.line("Redis is not configured...");
            return;
        }

        if (params.length > 1 && "unlock".equals(params[0])) {
            output.line("Killing lock: " + params[1]);
            redis.unlock(params[1], true);
        }

        output.line("Redis Statistics");
        output.separator();
        for (Map.Entry<String, String> e : redis.getInfo().entrySet()) {
            output.apply("%-40s %40s", e.getKey(), e.getValue());
        }

        output.blankLine();
        output.line("Redis Modules");
        output.separator();
        redis.getModules().forEach(output::line);

        output.blankLine();
        output.line("Redis Locks (use redis unlock <lock> to forcefully remove a lock)");
        output.separator();
        for (Redis.LockInfo info : redis.getLockList()) {
            output.apply("%-45s %-5s %-25s %-15s",
                         info.name,
                         info.value,
                         info.since != null ? info.since.toString() : "-",
                         info.ttl != null ? info.ttl + "s" : "-");
        }
    }

    @Override
    public String getDescription() {
        return "Reports statistics for Redis";
    }

    @Nonnull
    @Override
    public String getName() {
        return "redis";
    }
}
