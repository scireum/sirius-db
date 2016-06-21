/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.redis;

import com.google.common.collect.Lists;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import sirius.kernel.Lifecycle;
import sirius.kernel.async.CallContext;
import sirius.kernel.async.Operation;
import sirius.kernel.async.Tasks;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.commons.Wait;
import sirius.kernel.commons.Watch;
import sirius.kernel.di.PartCollection;
import sirius.kernel.di.std.ConfigValue;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Parts;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Average;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.Log;
import sirius.kernel.health.Microtiming;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Provides a thin layer to access Redis.
 * <p>
 * The connection parameters are setup using the system configuration. Most probably a value for <tt>redis.host</tt>
 * is all that is required. Another good practice is to use <tt>redis.db=1</tt> for developement system
 * so that they can run in parallel with a test system (which uses the default database 0 - unless configured
 * otherwise).
 */
@Register(classes = {Redis.class, Lifecycle.class})
public class Redis implements Lifecycle {

    @Parts(Subscriber.class)
    private PartCollection<Subscriber> subscribers;

    private List<JedisPubSub> subscriptions = Lists.newCopyOnWriteArrayList();
    private volatile AtomicBoolean subscriptionsActive = new AtomicBoolean(true);

    @Part
    private Tasks tasks;

    @Override
    public void started() {
        for (Subscriber subscriber : subscribers) {
            JedisPubSub subscription = new JedisPubSub() {
                @Override
                public void onMessage(String channel, String message) {
                    tasks.executor("redis-pubsub").start(() -> {
                        Watch w = Watch.start();
                        try {
                            subscriber.onMessage(message);
                        } catch (Throwable e) {
                            Exceptions.handle()
                                      .to(LOG)
                                      .error(e)
                                      .withSystemErrorMessage("Failed to process a message '%s' for topic '%s': %s (%s)",
                                                              message,
                                                              subscriber.getTopic())
                                      .handle();
                        }
                        w.submitMicroTiming("redis", channel);
                        messageDuration.addValue(w.elapsedMillis());
                    });
                }
            };
            subscriptions.add(subscription);
            new Thread(() -> subscribe(subscriber, subscription), "redis-subscriber-" + subscriber.getTopic()).start();
        }
    }

    private void subscribe(Subscriber subscriber, JedisPubSub subscription) {
        while (subscriptionsActive.get()) {
            try (Jedis redis = getConnection()) {
                LOG.INFO("Starting subscription for: %s", subscriber.getTopic());
                redis.subscribe(subscription, subscriber.getTopic());
                if (subscriptionsActive.get()) {
                    Wait.seconds(5);
                }
            } catch (Throwable e) {
                Exceptions.handle()
                          .to(LOG)
                          .error(e)
                          .withSystemErrorMessage("Failed to subscribe to a topic: %s (%s)")
                          .handle();
            }
            LOG.INFO("Terminated subscription for: %s", subscriber.getTopic());
        }
    }

    @Override
    public void stopped() {
        subscriptionsActive.set(false);
        for (JedisPubSub subscription : subscriptions) {
            try {
                subscription.unsubscribe();
            } catch (Throwable e) {
                Exceptions.handle()
                          .to(LOG)
                          .error(e)
                          .withSystemErrorMessage("Failed to unsubscribe from a topic: %s (%s)")
                          .handle();
            }
        }
        if (jedis != null) {
            jedis.close();
        }
    }

    @Override
    public void awaitTermination() {
    }

    @Override
    public String getName() {
        return "redis";
    }

    @ConfigValue("redis.host")
    private String host;

    @ConfigValue("redis.port")
    private int port;

    @ConfigValue("redis.timeout")
    private int timeout;

    @ConfigValue("redis.password")
    private String password;

    @ConfigValue("redis.db")
    private int db;

    @ConfigValue("redis.maxActive")
    private int maxActive;

    @ConfigValue("redis.maxIdle")
    private int maxIdle;

    public static final Log LOG = Log.get("redis");

    protected Average callDuration = new Average();
    protected Average messageDuration = new Average();
    protected JedisPool jedis;

    /**
     * Determines if access to Redis is configured.
     *
     * @return <tt>true</tt> if at least a host is given, <tt>false</tt> otherwise
     */
    public boolean isConfigured() {
        return Strings.isFilled(host);
    }

    private Jedis getConnection() {
        if (jedis == null) {
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(maxActive);
            jedisPoolConfig.setMaxIdle(maxIdle);
            jedis = new JedisPool(jedisPoolConfig,
                                  host,
                                  port,
                                  timeout,
                                  Strings.isFilled(password) ? password : null,
                                  db,
                                  CallContext.getNodeName());
        }

        return jedis.getResource();
    }

    /**
     * Executes one or more Redis commands and returns a value of the given type.
     *
     * @param description a description of the actions performed used for debugging and tracing
     * @param task        the actual task to perform using redis
     * @param <T>         the generic type of the result
     * @return a result computed by <tt>task</tt>
     */
    public <T> T query(Supplier<String> description, Function<Jedis, T> task) {
        Watch w = Watch.start();
        Operation op = Operation.create("redis", description, Duration.ofSeconds(10));
        try (Jedis redis = getConnection()) {
            return task.apply(redis);
        } catch (Throwable e) {
            throw Exceptions.handle(LOG, e);
        } finally {
            Operation.release(op);
            callDuration.addValue(w.elapsedMillis());
            if (Microtiming.isEnabled()) {
                w.submitMicroTiming("redis", description.get());
            }
        }
    }

    /**
     * Executes one or more Redis commands without any return value.
     *
     * @param description a description of the actions performed used for debugging and tracing
     * @param task        the actual task to perform using redis
     */
    public void exec(Supplier<String> description, Consumer<Jedis> task) {
        query(description, r -> {
            task.accept(r);
            return null;
        });
    }

    /**
     * Pushes a piece of data to a queue in Redis.
     *
     * @param queue the name of the queue
     * @param data  the data to push
     */
    public void pushToQueue(String queue, String data) {
        exec(() -> "Push to Queue: " + queue, r -> {
            r.lpush(queue, data);
        });
    }

    /**
     * Polls an element off a queue in Redis.
     *
     * @param queue the name of the queue
     * @return the next entry in the queue or <tt>null</tt> if the queue is empty
     */
    @Nullable
    public String pollQueue(String queue) {
        return query(() -> "Poll from Queue: " + queue, r -> {
            String result = r.rpop(queue);
            if (Strings.isEmpty(result)) {
                return null;
            } else {
                return result;
            }
        });
    }

    /**
     * Broadcasts a message to a pub-sub topic in redis.
     *
     * @param topic   the name of the topic to broadcast to
     * @param message the message to send
     */
    public void publish(String topic, String message) {
        exec(() -> "Publish to topic: " + topic, r -> {
            r.publish(topic, message);
        });
    }

    /**
     * Contains the entry name of the info section under which redis reports the amount of consumed ram
     */
    public static final String INFO_USED_MEMORY = "used_memory";

    /**
     * Returns a map of monitoring info about the redis server.
     *
     * @return a map containing statistical values supplied by the server
     */
    public Map<String, String> getInfo() {
        try {
            return Arrays.asList(query(() -> "info", Jedis::info).split("\n"))
                         .stream()
                         .map(l -> Strings.split(l, ":"))
                         .filter(t -> t.getFirst() != null && t.getSecond() != null)
                         .collect(Collectors.toMap(Tuple::getFirst, Tuple::getSecond));
        } catch (Exception e) {
            Exceptions.handle(LOG, e);
            return Collections.emptyMap();
        }
    }

    /**
     * Data object for storing information of a redis lock
     */
    public static class LockInfo {
        public String key;
        public String name;
        public String value;
        public LocalDateTime since;
        public Long ttl;
    }

    private static final String PREFIX_LOCK = "lock_";
    private static final String SUFFIX_DATE = "_date";

    /**
     * Returns a list of all currently held locks.
     * <p>
     * This is mainly inteded to be used for monitoring and maintenance (e.g. {@link RedisCommand})
     *
     * @return a list of all currently known locks
     */
    public List<LockInfo> getLockList() {
        List<LockInfo> result = Lists.newArrayList();
        exec(() -> "Get List of Locks", redis -> {
            for (String key : redis.keys(PREFIX_LOCK + "*")) {
                if (key.endsWith(SUFFIX_DATE)) {
                    continue;
                }
                String owner = redis.get(key);
                String since = redis.get(key + SUFFIX_DATE);
                Long ttl = redis.ttl(key);

                LockInfo info = new LockInfo();
                info.key = key;
                info.name = key.substring(PREFIX_LOCK.length());
                info.value = owner;
                if (Strings.isFilled(since)) {
                    info.since = LocalDateTime.parse(since);
                }
                if (ttl != null && ttl > -1) {
                    info.ttl = ttl;
                }
                result.add(info);
            }
        });

        return result;
    }

    /**
     * Tries to acquire the given lock in the given timeslot.
     * <p>
     * The system will try to acquire the given lock. If the lock is currently in use, it will retry
     * in 1 second intervals until either the lock is acquired or the <tt>acquireTimeout</tt> is over.
     * <p>
     * A sane value for the timeout might be in the range of 5-50s, highly depending on the algorithm
     * being protected by the lock. If the value is <tt>null</tt>, no retries will be performed.
     * <p>
     * The <tt>lockTimeout</tt> controls the max. age of the lock. After the given period, the lock
     * will be released, even if unlock wasn't called. This is to prevent a cluster from locking itself
     * out due to a single node crash. However, it is very important to chose a sane value here.
     *
     * @param lock           the name of the lock to acquire
     * @param acquireTimeout the max duration during which retires (in 1 second intervals) will be performed
     * @param lockTimeout    the max duration for which the lock will be kept before auto-releasing it
     * @return <tt>true</tt> if the lock was acquired, <tt>false</tt> otherwise
     */
    public boolean tryLock(@Nonnull String lock, @Nullable Duration acquireTimeout, @Nonnull Duration lockTimeout) {
        try {
            long timeout = acquireTimeout == null ? 0 : Instant.now().plus(acquireTimeout).toEpochMilli();
            int waitInMillis = 500;
            do {
                boolean locked = query(() -> "Try to Lock: " + lock, redis -> {
                    String key = PREFIX_LOCK + lock;
                    String response =
                            redis.set(key, CallContext.getNodeName(), "NX", "EX", (int) lockTimeout.getSeconds());
                    if ("OK".equals(response)) {
                        redis.set(key + SUFFIX_DATE,
                                  LocalDateTime.now().toString(),
                                  "NX",
                                  "EX",
                                  (int) lockTimeout.getSeconds());
                        return true;
                    }

                    return false;
                });

                if (locked) {
                    return true;
                }

                Wait.millis(waitInMillis);
                waitInMillis += 1000;
            } while (System.currentTimeMillis() < timeout);
            return false;
        } catch (Throwable e) {
            Exceptions.handle(LOG, e);
            return false;
        }
    }

    /**
     * Boilerplate method to perform the given task while holding the given lock.
     * <p>
     * See {@link #tryLock(String, Duration, Duration)} for details on acquiring a lock.
     * <p>
     * If the lock cannot be acquired, nothing will happen (neighter the task will be execute nor an exception will be
     * thrown).
     *
     * @param lock           the name of the lock to acquire
     * @param acquireTimeout the max duration during which retires (in 1 second intervals) will be performed
     * @param lockTimeout    the max duration for which the lock will be kept before auto-releasing it
     * @param lockedTask     the task to execute while holding the given lock. The task will not be executed if the
     *                       lock cannot be acquired within the given period
     */
    public void tryLocked(@Nonnull String lock,
                          @Nullable Duration acquireTimeout,
                          @Nonnull Duration lockTimeout,
                          @Nonnull Runnable lockedTask) {
        if (tryLock(lock, acquireTimeout, lockTimeout)) {
            try {
                lockedTask.run();
            } finally {
                unlock(lock);
            }
        }
    }

    /**
     * Determines if the given lock is currently locked by this or another node.
     *
     * @param lock the lock to check
     * @return <tt>true</tt> if the lock is currently active, <tt>false</tt> otherwise
     */
    public boolean isLocked(@Nonnull String lock) {
        return query(() -> "Check If Locked: " + lock, redis -> {
            String key = PREFIX_LOCK + lock;
            return redis.exists(key);
        });
    }

    /**
     * Releases the lock.
     *
     * @param lock the lock to release
     */
    public void unlock(String lock) {
        unlock(lock, false);
    }

    /**
     * Releases the given lock.
     *
     * @param lock  the lock to release
     * @param force if <tt>true</tt>, the lock will even be released if it is held by another node. This is a very
     *              dangerous operation and should only be used by maintenance and management tools like {@link
     *              RedisCommand}.
     */
    public void unlock(String lock, boolean force) {
        exec(() -> "Unlock: " + lock, redis -> {
            String key = PREFIX_LOCK + lock;
            String lockOwner = redis.get(key);
            if (force || Strings.areEqual(lockOwner, CallContext.getNodeName())) {
                redis.del(key);
                redis.del(key + SUFFIX_DATE);
            } else {
                if (lockOwner == null) {
                    LOG.WARN("Not going to unlock '%s' for '%s' as it seems to be expired already",
                             lock,
                             CallContext.getNodeName());
                } else {
                    LOG.WARN("Not going to unlock '%s' for '%s' as it is currently held by '%s'",
                             lock,
                             CallContext.getNodeName(),
                             lockOwner);
                }
            }
        });
    }
}
