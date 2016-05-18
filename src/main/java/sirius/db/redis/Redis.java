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

import javax.annotation.Nullable;
import java.time.Duration;
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
}
