package io.grove.quorumbreaker.redis;

import io.grove.quorumbreaker.cluster.AbstractClusterCoordinator;
import io.grove.quorumbreaker.gate.ClaimParams;
import io.grove.quorumbreaker.gate.WindowOutcome;
import io.grove.quorumbreaker.gate.WindowState;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisNoScriptException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import static io.grove.quorumbreaker.gate.WindowOutcome.EXPIRED;

@Slf4j
public final class RedisClusterCoordinator extends AbstractClusterCoordinator {

    private final StatefulRedisConnection<String, String> connection;
    private final StatefulRedisPubSubConnection<String, String> pubSubConnection;
    private final RedisCommands<String, String> sync;
    private final RedisPubSubCommands<String, String> pubSubSync;
    private final boolean ownsConnections;
    private final RedisClient ownedClient;
    private final String keyPrefix;
    private final String topicPrefix;
    private final Map<String, CopyOnWriteArrayList<Consumer<String>>> listenersByChannel = new ConcurrentHashMap<>();
    private final Object listenerRegistrationLock = new Object();
    private volatile String casSetSha;

    static final String NAMESPACE_PREFIX = "quorumbreaker:";
    public static final Duration DEFAULT_CALL_TIMEOUT = Duration.ofSeconds(1);

    private static final String CAS_SET_SCRIPT = """
            local current = redis.call('GET', KEYS[1])
            if current ~= ARGV[1] then
                return 0
            end
            if ARGV[3] == 'KEEPTTL' then
                redis.call('SET', KEYS[1], ARGV[2], 'KEEPTTL')
            else
                redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
            end
            return 1
            """;

    private RedisClusterCoordinator(StatefulRedisConnection<String, String> connection,
                                     StatefulRedisPubSubConnection<String, String> pubSubConnection,
                                     boolean ownsConnections, RedisClient ownedClient, String namespace,
                                     Duration callTimeout, Executor executor) {
        super(callTimeout, executor);
        requireNamespace(namespace);
        this.connection = connection;
        this.pubSubConnection = pubSubConnection;
        this.sync = connection.sync();
        this.pubSubSync = pubSubConnection.sync();
        this.ownsConnections = ownsConnections;
        this.ownedClient = ownedClient;
        this.keyPrefix = windowKeyPrefix(namespace);
        this.topicPrefix = topicPrefix(namespace);
        this.casSetSha = boundedJoin(callTimeout, CompletableFuture.supplyAsync(() -> sync.scriptLoad(CAS_SET_SCRIPT), executor));
        pubSubConnection.addListener(new RedisPubSubAdapter<>() {
            @Override
            public void message(String channel, String message) {
                List<Consumer<String>> listeners = listenersByChannel.get(channel);
                if (listeners == null) return;
                for (Consumer<String> listener : listeners) {
                    try {
                        listener.accept(message);
                    } catch (RuntimeException listenerFailed) {
                        log.warn("QuorumBreaker[{}]: a subscriber threw while handling a published message - " +
                                "continuing with the remaining subscribers", channel, listenerFailed);
                    }
                }
            }
        });
    }

    private static void requireNamespace(String namespace) {
        if (namespace == null || namespace.isBlank()) {
            throw new IllegalArgumentException("quorum-breaker namespace must not be blank - it keeps this " +
                    "application's shared Redis state from colliding with any other application on the " +
                    "same server/cluster");
        }
    }

    private static <T> T boundedJoin(Duration callTimeout, CompletionStage<T> stage) {
        try {
            return bounded(callTimeout, stage).toCompletableFuture().join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof RuntimeException re) throw re;
            throw e;
        }
    }

    static String windowKeyPrefix(String namespace) {
        return NAMESPACE_PREFIX + namespace + ":windows:";
    }

    static String topicPrefix(String namespace) {
        return NAMESPACE_PREFIX + namespace + ":events:";
    }

    public static RedisClusterCoordinator wrap(StatefulRedisConnection<String, String> connection,
                                                StatefulRedisPubSubConnection<String, String> pubSubConnection,
                                                String namespace) {
        return wrap(connection, pubSubConnection, namespace, DEFAULT_CALL_TIMEOUT);
    }

    public static RedisClusterCoordinator wrap(StatefulRedisConnection<String, String> connection,
                                                StatefulRedisPubSubConnection<String, String> pubSubConnection,
                                                String namespace, Duration callTimeout) {
        return wrap(connection, pubSubConnection, namespace, callTimeout, defaultExecutor());
    }

    public static RedisClusterCoordinator wrap(StatefulRedisConnection<String, String> connection,
                                                StatefulRedisPubSubConnection<String, String> pubSubConnection,
                                                String namespace, Duration callTimeout, Executor executor) {
        return new RedisClusterCoordinator(connection, pubSubConnection, false, null, namespace, callTimeout, executor);
    }

    public static RedisClusterCoordinator wrap(RedisClient client, String namespace) {
        return wrap(client, namespace, DEFAULT_CALL_TIMEOUT);
    }

    public static RedisClusterCoordinator wrap(RedisClient client, String namespace, Duration callTimeout) {
        return wrap(client, namespace, callTimeout, defaultExecutor());
    }

    public static RedisClusterCoordinator wrap(RedisClient client, String namespace, Duration callTimeout, Executor executor) {
        requireNamespace(namespace);
        StatefulRedisConnection<String, String> connection = null;
        StatefulRedisPubSubConnection<String, String> pubSubConnection = null;
        try {
            connection = client.connect();
            pubSubConnection = client.connectPubSub();
            return new RedisClusterCoordinator(connection, pubSubConnection, true, null, namespace, callTimeout, executor);
        } catch (RuntimeException e) {
            if (pubSubConnection != null) pubSubConnection.close();
            if (connection != null) connection.close();
            throw e;
        }
    }

    public static RedisClusterCoordinator client(RedisURI uri, String namespace) {
        return client(uri, namespace, DEFAULT_CALL_TIMEOUT);
    }

    public static RedisClusterCoordinator client(RedisURI uri, String namespace, Duration callTimeout) {
        return client(uri, namespace, callTimeout, defaultExecutor());
    }

    public static RedisClusterCoordinator client(RedisURI uri, String namespace, Duration callTimeout, Executor executor) {
        requireNamespace(namespace);
        RedisClient client = RedisClient.create(uri);
        StatefulRedisConnection<String, String> connection = null;
        StatefulRedisPubSubConnection<String, String> pubSubConnection = null;
        try {
            connection = client.connect();
            pubSubConnection = client.connectPubSub();
            return new RedisClusterCoordinator(connection, pubSubConnection, true, client, namespace, callTimeout, executor);
        } catch (RuntimeException e) {
            if (pubSubConnection != null) pubSubConnection.close();
            if (connection != null) connection.close();
            client.shutdown();
            throw e;
        }
    }

    @Override
    public CompletionStage<Void> publish(String channel, String message) {
        return bounded(callTimeout, CompletableFuture.runAsync(() -> sync.publish(topicPrefix + channel, message), executor));
    }

    @Override
    public CompletionStage<AutoCloseable> subscribe(String channel, Consumer<String> listener) {
        String fullChannel = topicPrefix + channel;
        return subscribeBounded(channel, () -> {
            synchronized (listenerRegistrationLock) {
                CopyOnWriteArrayList<Consumer<String>> listeners =
                        listenersByChannel.computeIfAbsent(fullChannel, ignored -> new CopyOnWriteArrayList<>());
                if (listeners.isEmpty()) {
                    pubSubSync.subscribe(fullChannel);
                }
                listeners.add(listener);
            }
            return (AutoCloseable) () -> unsubscribe(fullChannel, listener);
        });
    }

    private void unsubscribe(String fullChannel, Consumer<String> listener) {
        synchronized (listenerRegistrationLock) {
            CopyOnWriteArrayList<Consumer<String>> listeners = listenersByChannel.get(fullChannel);
            if (listeners == null) return;
            listeners.remove(listener);
            if (listeners.isEmpty()) {
                listenersByChannel.remove(fullChannel);
                pubSubSync.unsubscribe(fullChannel);
            }
        }
    }

    @Override
    protected ClaimResult claimOnce(String key, ClaimParams params, long ttlMillis) {
        String fullKey = keyPrefix + key;
        while (true) {
            String current = sync.get(fullKey);
            if (current == null) {
                WindowState fresh = WindowState.fresh(UUID.randomUUID().toString()).claim();
                String result = sync.set(fullKey, fresh.toString(), SetArgs.Builder.nx().px(ttlMillis));
                if ("OK".equals(result)) {
                    return new ClaimResult(true, fresh.generation());
                }
                continue;
            }
            WindowState state = WindowState.parse(current);
            Optional<WindowState> next = state.tryClaim(params);
            if (next.isEmpty()) return new ClaimResult(false, state.generation());
            if (casSet(fullKey, current, next.get().toString(), ttlMillis)) {
                return new ClaimResult(true, state.generation());
            }
        }
    }

    @Override
    protected void release(String key, String generation) {
        String fullKey = keyPrefix + key;
        while (true) {
            String current = sync.get(fullKey);
            if (current == null) return;
            WindowState state = WindowState.parse(current);
            if (!state.generation().equals(generation)) return;
            if (casSetKeepTtl(fullKey, current, state.release().toString())) return;
        }
    }

    @Override
    protected WindowOutcome traceOnce(String key, ClaimParams params, boolean success, boolean slow, long ttlMillis) {
        String fullKey = keyPrefix + key;
        while (true) {
            String current = sync.get(fullKey);
            if (current == null) return EXPIRED;

            WindowState before = WindowState.parse(current);
            WindowState after = before.recordOutcome(success, slow);

            if (!casSet(fullKey, current, after.toString(), ttlMillis)) {
                continue;
            }
            return before.decide(after, params);
        }
    }

    @Override
    protected CompletionStage<Void> evict(String key) {
        return CompletableFuture.runAsync(() -> sync.del(keyPrefix + key), executor);
    }

    private boolean casSet(String fullKey, String expected, String newValue, long ttlMillis) {
        return casSet(fullKey, expected, newValue, String.valueOf(ttlMillis));
    }

    private boolean casSetKeepTtl(String fullKey, String expected, String newValue) {
        return casSet(fullKey, expected, newValue, "KEEPTTL");
    }

    private boolean casSet(String fullKey, String expected, String newValue, String ttlArg) {
        Long result;
        try {
            result = sync.evalsha(casSetSha, ScriptOutputType.INTEGER, new String[]{fullKey}, expected, newValue, ttlArg);
        } catch (RedisNoScriptException notCached) {
            casSetSha = sync.scriptLoad(CAS_SET_SCRIPT);
            result = sync.eval(CAS_SET_SCRIPT, ScriptOutputType.INTEGER, new String[]{fullKey}, expected, newValue, ttlArg);
        }
        return result != null && result == 1L;
    }

    @Override
    public void close() {
        if (ownsConnections) {
            connection.close();
            pubSubConnection.close();
        }
        if (ownedClient != null) {
            ownedClient.shutdown();
        }
    }

}
