/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.redis;

import redis.clients.jedis.ClientSetInfoConfig;
import redis.clients.jedis.ConnectionPoolConfig;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.RedisSentinelClient;
import redis.clients.jedis.UnifiedJedis;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    protected UnifiedJedis jedis;

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

        if (jedis != null) {
            UnifiedJedis copy = this.jedis;
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
     * @return access to the managed Redis client.
     */
    public UnifiedJedis getConnection() {
        if (jedis != null) {
            return jedis;
        }

        return setupConnection();
    }

    private synchronized UnifiedJedis setupConnection() {
        if (jedis != null) {
            return jedis;
        }

        if (Strings.isFilled(sentinels)) {
            jedis = RedisSentinelClient.builder()
                                       .masterName(masterName)
                                       .sentinels(parseSentinels())
                                       .clientConfig(createClientConfig())
                                       .sentinelClientConfig(createClientConfig())
                                       .poolConfig(createPoolConfig())
                                       .build();
            return jedis;
        }

        if (Strings.isEmpty(host)) {
            Redis.LOG.SEVERE(Strings.apply(
                    "Missing a Redis host for config '%s'! This might lead to undefined behaviour."
                    + " Please specify redis.host or redis.sentinels!",
                    name));
        }

        Tuple<String, Integer> effectiveHostAndPort = PortMapper.mapPort(determineServiceName(), host, port);
        HostAndPort hostAndPort = new HostAndPort(effectiveHostAndPort.getFirst(), effectiveHostAndPort.getSecond());

        jedis = RedisClient.builder()
                           .hostAndPort(hostAndPort)
                           .clientConfig(createClientConfig())
                           .poolConfig(createPoolConfig())
                           .build();

        return jedis;
    }

    private Set<HostAndPort> parseSentinels() {
        return Arrays.stream(sentinels.split(","))
                     .map(String::trim)
                     .filter(Strings::isFilled)
                     .map(this::parseHostAndPort)
                     .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private HostAndPort parseHostAndPort(String hostAndPort) {
        int separator = hostAndPort.lastIndexOf(':');
        if (separator < 0 || separator == hostAndPort.length() - 1) {
            return new HostAndPort(hostAndPort, 26379);
        }

        String sentinelHost = hostAndPort.substring(0, separator);
        int sentinelPort = Integer.parseInt(hostAndPort.substring(separator + 1));
        return new HostAndPort(sentinelHost, sentinelPort);
    }

    private ConnectionPoolConfig createPoolConfig() {
        ConnectionPoolConfig poolConfig = new ConnectionPoolConfig();
        poolConfig.setMaxTotal(maxActive);
        poolConfig.setMaxIdle(maxIdle);
        return poolConfig;
    }

    private DefaultJedisClientConfig createClientConfig() {
        return DefaultJedisClientConfig.builder()
                                       .database(db)
                                       .clientName(CallContext.getNodeName())
                                       .connectionTimeoutMillis(connectTimeout)
                                       .socketTimeoutMillis(readTimeout)
                                       .clientSetInfoConfig(new ClientSetInfoConfig(!enableClientInfo))
                                       .password(Strings.isFilled(password) ? password : null)
                                       .build();
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
    public <T> T query(Supplier<String> description, Function<UnifiedJedis, T> task) {
        Watch w = Watch.start();
        try (var _ = new Operation(description, Duration.ofSeconds(10))) {
            UnifiedJedis redis = getConnection();
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
    public void exec(Supplier<String> description, Consumer<UnifiedJedis> task) {
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
            return Arrays.stream(query(() -> "info", UnifiedJedis::info).split("\n"))
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

    /**
     * Returns a list of all loaded modules in the redis server.
     *
     * @return a list of module data as reported by redis INFO command
     */
    public List<String> getModules() {
        try {
            return Arrays.stream(query(() -> "info", UnifiedJedis::info).split("\n"))
                         .map(line -> Strings.split(line, ":"))
                         .filter(keyAndValue -> INFO_MODULE.equals(keyAndValue.getFirst()))
                         .map(Tuple::getSecond)
                         .toList();
        } catch (Exception exception) {
            Exceptions.handle(Redis.LOG, exception);
            return Collections.emptyList();
        }
    }
}
