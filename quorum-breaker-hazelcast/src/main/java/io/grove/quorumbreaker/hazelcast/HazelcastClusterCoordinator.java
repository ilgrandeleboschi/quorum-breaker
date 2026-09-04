package io.grove.quorumbreaker.hazelcast;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.topic.ITopic;
import io.grove.quorumbreaker.cluster.AbstractClusterCoordinator;
import io.grove.quorumbreaker.gate.ClaimParams;
import io.grove.quorumbreaker.gate.WindowOutcome;
import io.grove.quorumbreaker.gate.WindowState;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.grove.quorumbreaker.gate.WindowOutcome.EXPIRED;

@Slf4j
public final class HazelcastClusterCoordinator extends AbstractClusterCoordinator {

    private final HazelcastInstance instance;
    private final boolean ownsInstance;
    private final IMap<String, String> windows;
    private final String topicPrefix;

    static final String NAMESPACE_PREFIX = "quorumbreaker:";
    public static final Duration DEFAULT_CALL_TIMEOUT = Duration.ofSeconds(1);

    private HazelcastClusterCoordinator(HazelcastInstance instance, boolean ownsInstance, String namespace,
                                         Duration callTimeout, Executor executor) {
        super(callTimeout, executor);
        if (namespace == null || namespace.isBlank()) {
            throw new IllegalArgumentException("quorum-breaker namespace must not be blank - it keeps this " +
                    "application's shared Hazelcast state from colliding with any other application on the " +
                    "same cluster");
        }
        this.instance = instance;
        this.ownsInstance = ownsInstance;
        this.windows = instance.getMap(windowsMapName(namespace));
        this.topicPrefix = topicPrefix(namespace);
    }

    static String windowsMapName(String namespace) {
        return NAMESPACE_PREFIX + namespace + ":windows";
    }

    static String topicPrefix(String namespace) {
        return NAMESPACE_PREFIX + namespace + ":events:";
    }

    public static HazelcastClusterCoordinator wrap(HazelcastInstance instance, String namespace) {
        return wrap(instance, namespace, DEFAULT_CALL_TIMEOUT);
    }

    public static HazelcastClusterCoordinator wrap(HazelcastInstance instance, String namespace, Duration callTimeout) {
        return wrap(instance, namespace, callTimeout, defaultExecutor());
    }

    public static HazelcastClusterCoordinator wrap(HazelcastInstance instance, String namespace,
                                                    Duration callTimeout, Executor executor) {
        return new HazelcastClusterCoordinator(instance, false, namespace, callTimeout, executor);
    }

    public static HazelcastClusterCoordinator embedded(Config config, String namespace) {
        return embedded(config, namespace, DEFAULT_CALL_TIMEOUT);
    }

    public static HazelcastClusterCoordinator embedded(Config config, String namespace, Duration callTimeout) {
        return embedded(config, namespace, callTimeout, defaultExecutor());
    }

    public static HazelcastClusterCoordinator embedded(Config config, String namespace,
                                                        Duration callTimeout, Executor executor) {
        return new HazelcastClusterCoordinator(Hazelcast.newHazelcastInstance(config), true, namespace, callTimeout, executor);
    }

    public static HazelcastClusterCoordinator client(ClientConfig config, String namespace) {
        return client(config, namespace, DEFAULT_CALL_TIMEOUT);
    }

    public static HazelcastClusterCoordinator client(ClientConfig config, String namespace, Duration callTimeout) {
        return client(config, namespace, callTimeout, defaultExecutor());
    }

    public static HazelcastClusterCoordinator client(ClientConfig config, String namespace,
                                                      Duration callTimeout, Executor executor) {
        return new HazelcastClusterCoordinator(HazelcastClient.newHazelcastClient(config), true, namespace, callTimeout, executor);
    }

    @Override
    public CompletionStage<Void> publish(String channel, String message) {
        return bounded(callTimeout, CompletableFuture.runAsync(() ->
                instance.getTopic(topicPrefix + channel).publish(message), executor));
    }

    @Override
    public CompletionStage<AutoCloseable> subscribe(String channel, Consumer<String> listener) {
        return subscribeBounded(channel, () -> {
            ITopic<String> topic = instance.getTopic(topicPrefix + channel);
            UUID registrationId = topic.addMessageListener(message -> listener.accept(message.getMessageObject()));
            return (AutoCloseable) () -> topic.removeMessageListener(registrationId);
        });
    }

    @Override
    protected ClaimResult claimOnce(String key, ClaimParams params, long ttlMillis) {
        while (true) {
            String current = windows.get(key);
            if (current == null) {
                WindowState fresh = WindowState.fresh(UUID.randomUUID().toString()).claim();
                if (windows.putIfAbsent(key, fresh.toString(), ttlMillis, TimeUnit.MILLISECONDS) == null) {
                    return new ClaimResult(true, fresh.generation());
                }
                continue;
            }
            WindowState state = WindowState.parse(current);
            Optional<WindowState> next = state.tryClaim(params);
            if (next.isEmpty()) return new ClaimResult(false, state.generation());
            if (windows.replace(key, current, next.get().toString())) {
                touchTtlQuietly(key, ttlMillis);
                return new ClaimResult(true, state.generation());
            }
        }
    }

    @Override
    protected void release(String key, String generation) {
        while (true) {
            String current = windows.get(key);
            if (current == null) return;
            WindowState state = WindowState.parse(current);
            if (!state.generation().equals(generation)) return;
            if (windows.replace(key, current, state.release().toString())) return;
        }
    }

    @Override
    protected WindowOutcome traceOnce(String key, ClaimParams params, boolean success, boolean slow, long ttlMillis) {
        while (true) {
            String current = windows.get(key);
            if (current == null) return EXPIRED;

            WindowState before = WindowState.parse(current);
            WindowState after = before.recordOutcome(success, slow);

            if (!windows.replace(key, current, after.toString())) {
                continue;
            }
            touchTtlQuietly(key, ttlMillis);
            return before.decide(after, params);
        }
    }

    @Override
    protected CompletionStage<Void> evict(String key) {
        return CompletableFuture.runAsync(() -> windows.delete(key), executor);
    }

    private void touchTtlQuietly(String key, long ttlMillis) {
        try {
            windows.setTtl(key, ttlMillis, TimeUnit.MILLISECONDS);
        } catch (RuntimeException ttlRefreshFailed) {
            log.warn("QuorumBreaker[{}]: claimed/updated the shared window but failed to refresh its TTL - " +
                    "it may expire earlier than intended", key, ttlRefreshFailed);
        }
    }

    @Override
    public void close() {
        if (ownsInstance) instance.shutdown();
    }

}
