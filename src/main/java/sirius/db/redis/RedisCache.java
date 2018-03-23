/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.redis;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.primitives.Ints;
import sirius.kernel.Sirius;
import sirius.kernel.cache.Cache;
import sirius.kernel.cache.CacheEntry;
import sirius.kernel.cache.CacheManager;
import sirius.kernel.cache.ValueComputer;
import sirius.kernel.cache.ValueVerifier;
import sirius.kernel.cache.distributed.ValueParser;
import sirius.kernel.commons.Callback;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.di.std.Part;
import sirius.kernel.health.Counter;
import sirius.kernel.health.Exceptions;
import sirius.kernel.settings.Extension;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

/**
 * A distributed {@link Cache} backed by Redis. Cached data is the same across nodes for distributed applications.
 */
public class RedisCache<V> implements Cache<String, V> {

    private static final String CACHE_PREFIX = "cache-";
    private static final int MAX_HISTORY = 25;
    private static final double ONE_HUNDERT_PERCENT = 100d;

    private static final String EXTENSION_TYPE_CACHE = "cache";
    private static final String CONFIG_KEY_MAX_SIZE = "maxSize";
    private static final String CONFIG_KEY_TTL = "ttl";
    private static final String CONFIG_KEY_VERIFICATION = "verification";

    @Part
    private static Redis redis;

    private final String name;
    private final ValueComputer<String, V> valueComputer;
    private final ValueVerifier<V> verifier;
    private final ValueParser<V> valueParser;

    private final long verificationInterval;
    private final long timeToLive;
    private final Integer maxSize;

    private final Counter hits = new Counter();
    private final Counter misses = new Counter();
    private final List<Long> usesHistory = new ArrayList<>(MAX_HISTORY);
    private final List<Long> hitRateHistory = new ArrayList<>(MAX_HISTORY);
    private Callback<Tuple<String, V>> removeListener;
    private Date lastEvictionRun;

    public RedisCache(@Nonnull String name,
                      @Nullable ValueComputer<String, V> valueComputer,
                      @Nullable ValueVerifier<V> verifier,
                      @Nonnull ValueParser<V> valueParser) {
        this.name = name;
        this.valueComputer = valueComputer;
        this.verifier = verifier;
        this.valueParser = valueParser;

        Extension cacheInfo = Sirius.getSettings().getExtension(EXTENSION_TYPE_CACHE, name);
        if (cacheInfo.isDefault()) {
            CacheManager.LOG.WARN("Cache %s does not exist! Using defaults...", name);
        }
        this.verificationInterval = cacheInfo.getMilliseconds(CONFIG_KEY_VERIFICATION);
        this.timeToLive = cacheInfo.getMilliseconds(CONFIG_KEY_TTL);
        this.maxSize = cacheInfo.get(CONFIG_KEY_MAX_SIZE).getInteger();
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
        return hits.getCount() + misses.getCount();
    }

    @Override
    public List<Long> getUseHistory() {
        return usesHistory;
    }

    @Override
    public Long getHitRate() {
        long h = hits.getCount();
        long m = misses.getCount();
        return h + m == 0L ? 0L : Math.round(ONE_HUNDERT_PERCENT * h / (h + m));
    }

    @Override
    public List<Long> getHitRateHistory() {
        return hitRateHistory;
    }

    @Override
    public Date getLastEvictionRun() {
        return lastEvictionRun;
    }

    @Override
    public void clear() {
        redis.exec(() -> "Clearing " + getCacheName(), jedis -> jedis.del(getCacheName()));
        hits.reset();
        misses.reset();
    }

    @Nullable
    @Override
    public V get(@Nonnull String key) {
        return get(key, valueComputer);
    }

    @Nullable
    @Override
    public V get(@Nonnull String key, @Nullable ValueComputer<String, V> computer) {
        CacheEntry<String, V> entry = getEntryFromJSON(getStringFromRedis(key));

        if (entry != null) {
            entry = verifyEntry(entry);
        }

        if (entry == null) {
            if (computer != null) {
                entry = putAndCreateEntry(key, computer.compute(key));
            }
            misses.inc();
        } else {
            hits.inc();
        }

        if (entry != null) {
            return entry.getValue();
        } else {
            return null;
        }
    }

    private String getStringFromRedis(@Nonnull String key) {
        return redis.query(() -> "Getting from cache " + getCacheName(), jedis -> jedis.hget(getCacheName(), key));
    }

    private CacheEntry<String, V> verifyEntry(CacheEntry<String, V> entry) {
        long now = System.currentTimeMillis();

        // Verify age of entry
        if (entry.getMaxAge() > 0 && entry.getMaxAge() < now) {
            remove(entry.getKey());
            return null;
        }

        // Apply verifier if present
        if (verifier != null && verificationInterval > 0 && entry.getNextVerification() < now) {
            if (!verifier.valid(entry.getValue())) {
                remove(entry.getKey());
                return null;
            }
        }
        return entry;
    }

    private CacheEntry<String, V> getEntryFromJSON(String result) {
        if (Strings.isEmpty(result)) {
            return null;
        }
        JSONObject parseObject = JSON.parseObject(result);
        long created = parseObject.getLong("created");
        long used = parseObject.getLong("used");
        String key = parseObject.getString("key");
        V value = valueParser.toObject(parseObject.getString("value"));
        long maxAge = parseObject.getLong("maxAge");
        long nextVerification = parseObject.getLong("nextVerification");
        CacheEntry<String, V> cacheEntry = new CacheEntry<>(key, value, maxAge, nextVerification);
        cacheEntry.setCreated(created);
        cacheEntry.setUsed(used);
        return cacheEntry;
    }

    @Override
    public void put(@Nonnull String key, @Nullable V value) {
        putAndCreateEntry(key, value);
    }

    private CacheEntry<String, V> putAndCreateEntry(@Nonnull String key, @Nullable V value) {
        long now = System.currentTimeMillis();

        CacheEntry<String, String> entryForRedis =
                new CacheEntry<>(key, valueParser.toJSON(value), now + timeToLive, now + verificationInterval);
        String newEntryJSON = JSON.toJSONString(entryForRedis);
        redis.exec(() -> "Putting in cache " + getCacheName(), jedis -> jedis.hset(getCacheName(), key, newEntryJSON));

        return new CacheEntry<>(key, value, now + timeToLive, now + verificationInterval);
    }

    @Override
    public void remove(@Nonnull String key) {
        CacheEntry<String, V> currentValue = getEntryFromJSON(getStringFromRedis(key));
        if (currentValue == null) {
            return;
        }

        redis.exec(() -> "Removing from cache " + getCacheName(), jedis -> jedis.hdel(getCacheName(), key));

        if (removeListener == null) {
            return;
        }
        try {
            removeListener.invoke(Tuple.create(currentValue.getKey(), currentValue.getValue()));
        } catch (Exception e) {
            Exceptions.handle(e);
        }
    }

    @Override
    public void removeIf(@Nonnull Predicate<CacheEntry<String, V>> predicate) {
        for (CacheEntry<String, V> entry : getContents()) {
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
    public List<CacheEntry<String, V>> getContents() {
        List<CacheEntry<String, V>> cacheEntries = new ArrayList<>();
        redis.query(() -> "Get all from " + getCacheName(), jedis -> jedis.hgetAll(getCacheName()))
             .forEach((key, value) -> cacheEntries.add(getEntryFromJSON(value)));
        return cacheEntries;
    }

    @Override
    public Cache<String, V> onRemove(Callback<Tuple<String, V>> onRemoveCallback) {
        removeListener = onRemoveCallback;
        return this;
    }

    @Override
    public void updateStatistics() {
        usesHistory.add(getUses());
        if (usesHistory.size() > MAX_HISTORY) {
            usesHistory.remove(0);
        }
        hitRateHistory.add(getHitRate());
        if (hitRateHistory.size() > MAX_HISTORY) {
            hitRateHistory.remove(0);
        }
        hits.reset();
        misses.reset();
    }

    @Override
    public void runEviction() {
        if (timeToLive <= 0) {
            return;
        }

        lastEvictionRun = new Date();
        long now = System.currentTimeMillis();
        int numEvicted = 0;
        for (CacheEntry<String, V> entry : getContents()) {
            if (entry.getMaxAge() < now) {
                remove(entry.getKey());
                numEvicted++;
            }
        }
        if (numEvicted > 0 && CacheManager.LOG.isFINE()) {
            CacheManager.LOG.FINE("Evicted %d entries from %s", numEvicted, name);
        }
    }
}
