/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.redis;

import com.google.common.primitives.Ints;
import sirius.kernel.cache.Cache;
import sirius.kernel.cache.CacheEntry;
import sirius.kernel.cache.ValueComputer;
import sirius.kernel.cache.ValueVerifier;
import sirius.kernel.commons.Callback;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.di.std.Part;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

public class RedisCache implements Cache<String, String> {

    private static final String CACHE_PREFIX = "cache-";

    @Part
    private static Redis redis;

    private String name;
    private ValueComputer<String, String> valueComputer;
    private ValueVerifier<String> verifier;

    public RedisCache(String name, ValueComputer<String, String> valueComputer, ValueVerifier<String> verifier) {
        this.name = name;
        this.valueComputer = valueComputer;
        this.verifier = verifier;
    }

    @Override
    public String getName() {
        return name;
    }

    private String getCacheName() {
        return CACHE_PREFIX + getName();
    }

    @Override
    public int getMaxSize() {
        return 0;
    }

    @Override
    public int getSize() {
        return Ints.checkedCast(redis.query(() -> "Getting size of cache " + getCacheName(),
                                            jedis -> jedis.hlen(getCacheName())));
    }

    @Override
    public long getUses() {
        return 0;
    }

    @Override
    public List<Long> getUseHistory() {
        return null;
    }

    @Override
    public Long getHitRate() {
        return null;
    }

    @Override
    public List<Long> getHitRateHistory() {
        return null;
    }

    @Override
    public Date getLastEvictionRun() {
        return null;
    }

    @Override
    public void clear() {
        redis.exec(() -> "Clearing " + getCacheName(), jedis -> jedis.del(getCacheName()));
    }

    @Nullable
    @Override
    public String get(@Nonnull String key) {
        return get(key, valueComputer);
    }

    @Nullable
    @Override
    public String get(@Nonnull String key, @Nullable ValueComputer<String, String> computer) {
        String result =
                redis.query(() -> "Getting from cache " + getCacheName(), jedis -> jedis.hget(getCacheName(), key));
        if (Strings.isEmpty(result) && computer != null) {
            result = computer.compute(key);
            put(key, result);
        }
        return result;
    }

    @Override
    public void put(@Nonnull String key, @Nullable String value) {
        redis.exec(() -> "Putting in cache " + getCacheName(), jedis -> jedis.hset(getCacheName(), key, value));
    }

    @Override
    public void remove(@Nonnull String key) {
        redis.exec(() -> "Removing from cache " + getCacheName(), jedis -> jedis.hdel(getCacheName(), key));
    }

    @Override
    public void removeIf(@Nonnull Predicate<CacheEntry<String, String>> predicate) {
        for (CacheEntry<String, String> entry : getContents()) {
            if (predicate.test(entry)) {
                remove(entry.getKey());
            }
        }
    }

    @Override
    public boolean contains(@Nonnull String key) {
        return redis.query(() -> "Checkin if exists in cache " + getCacheName(),
                           jedis -> jedis.hexists(getCacheName(), key));
    }

    @Override
    public Iterator<String> keySet() {
        return redis.query(() -> "Get keys from" + getCacheName(), jedis -> jedis.hkeys(getCacheName())).iterator();
    }

    @Override
    public List<CacheEntry<String, String>> getContents() {
        List<CacheEntry<String, String>> cacheEntries = new ArrayList<>();
        redis.query(() -> "Get all from " + getCacheName(), jedis -> jedis.hgetAll(getCacheName()))
             .forEach((key, value) -> cacheEntries.add(new CacheEntry<>(key, value, 0, 0)));
        return cacheEntries;
    }

    @Override
    public Cache<String, String> onRemove(Callback<Tuple<String, String>> onRemoveCallback) {
        return null;
    }

    @Override
    public void updateStatistics() {

    }

    @Override
    public void runEviction() {

    }
}
