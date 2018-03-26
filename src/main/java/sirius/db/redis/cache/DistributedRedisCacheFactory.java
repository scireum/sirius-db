/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.redis.cache;

import sirius.db.redis.Redis;
import sirius.kernel.cache.Cache;
import sirius.kernel.cache.ValueComputer;
import sirius.kernel.cache.ValueVerifier;
import sirius.kernel.cache.distributed.DistributedCacheFactory;
import sirius.kernel.cache.distributed.ValueParser;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;

/**
 * Creates a distributed cache backed by redis.
 */
@Register
public class DistributedRedisCacheFactory implements DistributedCacheFactory {

    @Part
    private static Redis redis;

    @Override
    public <V> Cache<String, V> createDistributedCache(String name,
                                                       ValueComputer<String, V> valueComputer,
                                                       ValueVerifier<V> verifier,
                                                       ValueParser<V> valueParser) {
        return new RedisCache<>(name, valueComputer, verifier, valueParser);
    }

    @Override
    public boolean isConfigured() {
        return redis.isConfigured();
    }
}
