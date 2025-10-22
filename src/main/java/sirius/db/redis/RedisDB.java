/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.redis;

import redis.clients.jedis.ClientSetInfoConfig;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import sirius.kernel.async.CallContext;
import sirius.kernel.async.Operation;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.commons.Watch;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.Microtiming;
import sirius.kernel.settings.Extension;
import sirius.kernel.settings.PortMapper;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Represents a connection pool to either a redis database or a set of sentinels which elect a master.
 * <p>
 * An instance is obtained via {@link Redis#getPool(String)} or {@link Redis#getSystem()}.
 */
public class RedisDB {

    private static final String INFO_MODULE = "module";

    private final Redis redisInstance;
    private final String name;
    private final String host;
    private final int port;
    private final int connectTimeout;
    private final int readTimeout;
    private final String password;
    private final int db;
    private final int maxActive;
    private final int maxIdle;
    private final String masterName;
    private final String sentinels;
    private boolean available = true;

    /**
     * Determines whether additional client information should be sent to the server when connecting.
     * <p>
     * This may be disabled for some redis servers that do not support this feature.
     */
    private final boolean enableClientInfo;

    protected JedisPool jedis;
    protected JedisSentinelPool sentinelPool;

    protected RedisDB(Redis redisInstance, Extension config) {
        this.redisInstance = redisInstance;
        this.name = config.getId();
        this.host = config.getString("host");
        this.port = config.getInt("port");
        this.connectTimeout = config.getInt("connectTimeout");
        this.readTimeout = config.getInt("readTimeout");
        this.password = config.getString("password");
        this.db = config.getInt("db");
        this.maxActive = config.getInt("maxActive");
        this.maxIdle = config.getInt("maxIdle");
        this.masterName = config.getString("masterName");
        this.sentinels = config.getString("sentinels");
        this.enableClientInfo = config.get("enableClientInfo").asBoolean(false);
    }

    /**
     * Determines if access to Redis is configured.
     *
     * @return <tt>true</tt> if at least a host is given or at least one sentinel is available, <tt>false</tt> otherwise
     */
    public boolean isConfigured() {
        return available && (Strings.isFilled(host) || Strings.isFilled(sentinels));
    }

    protected void close() {
        available = false;

        if (sentinelPool != null) {
            JedisSentinelPool copy = this.sentinelPool;
            this.sentinelPool = null;
            copy.close();
        }

        if (jedis != null) {
            JedisPool copy = this.jedis;
            this.jedis = null;
            copy.close();
        }
    }

    /**
     * Provides raw access to the underlying Redis connection.
     * <p>
     * Note that this method should be used with absolute care and calling {@link #query(Supplier, Function)}
     * or {@link #exec(Supplier, Consumer)} is preferred as it ensures monitoring and proper connection handling.
     *
     * @return access to a Redis connection from the managed pool. Note that {@link Jedis#close()} has to be called
     * in any case to ensure that the connection is returned to the pool.
     */
    public Jedis getConnection() {
        if (sentinelPool != null) {
            return sentinelPool.getResource();
        }
        if (jedis != null) {
            return jedis.getResource();
        }

        return setupConnection();
    }

    private synchronized Jedis setupConnection() {
        if (sentinelPool == null && Strings.isFilled(sentinels)) {
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(maxActive);
            poolConfig.setMaxIdle(maxIdle);

            sentinelPool = new JedisSentinelPool(masterName,
                                                 Arrays.stream(sentinels.split(","))
                                                       .map(String::trim)
                                                       .collect(Collectors.toSet()),
                                                 poolConfig,
                                                 connectTimeout,
                                                 readTimeout,
                                                 null,
                                                 db);
            return sentinelPool.getResource();
        }
        if (sentinelPool != null) {
            return sentinelPool.getResource();
        }

        if (jedis == null) {
            if (Strings.isEmpty(host)) {
                Redis.LOG.SEVERE(Strings.apply(
                        "Missing a Redis host for config '%s'! This might lead to undefined behaviour."
                        + " Please specify redis.host or redis.sentinels!",
                        name));
            }

            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(maxActive);
            jedisPoolConfig.setMaxIdle(maxIdle);

            Tuple<String, Integer> effectiveHostAndPort = PortMapper.mapPort(determineServiceName(), host, port);
            HostAndPort hostAndPort =
                    new HostAndPort(effectiveHostAndPort.getFirst(), effectiveHostAndPort.getSecond());

            DefaultJedisClientConfig jedisClientConfig = DefaultJedisClientConfig.builder()
                                                                                 .database(db)
                                                                                 .clientName(CallContext.getNodeName())
                                                                                 .connectionTimeoutMillis(connectTimeout)
                                                                                 .socketTimeoutMillis(readTimeout)
                                                                                 .clientSetInfoConfig(new ClientSetInfoConfig(
                                                                                         !enableClientInfo))
                                                                                 .password(Strings.isFilled(password) ?
                                                                                           password :
                                                                                           null)
                                                                                 .build();

            jedis = new JedisPool(jedisPoolConfig, hostAndPort, jedisClientConfig);
        }

        return jedis.getResource();
    }

    private String determineServiceName() {
        return Redis.POOL_SYSTEM.equals(name) ? "redis" : "redis-" + name;
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
        try (var _ = new Operation(description, Duration.ofSeconds(10)); Jedis redis = getConnection()) {
            return task.apply(redis);
        } catch (Exception exception) {
            throw Exceptions.handle(Redis.LOG, exception);
        } finally {
            redisInstance.callDuration.addValue(w.elapsedMillis());
            if (Microtiming.isEnabled()) {
                w.submitMicroTiming("REDIS", description.get());
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
     * Returns a map of monitoring info about the redis server.
     *
     * @return a map containing statistical values supplied by the server
     */
    public Map<String, String> getInfo() {
        try {
            return Arrays.stream(query(() -> "info", Jedis::info).split("\n"))
                         .map(line -> Strings.split(line, ":"))
                         .filter(keyAndValue -> Strings.areAllFilled(keyAndValue.getFirst(), keyAndValue.getSecond()))
                         // Modules are listed under the same "key", so we skip them from here
                         .filter(keyAndValue -> !INFO_MODULE.equals(keyAndValue.getFirst()))
                         .collect(Collectors.toMap(Tuple::getFirst, Tuple::getSecond));
        } catch (Exception exception) {
            Exceptions.handle(Redis.LOG, exception);
            return Collections.emptyMap();
        }
    }
}
