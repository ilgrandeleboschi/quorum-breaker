package io.grove.quorumbreaker.breaker;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.circuitbreaker.IllegalStateTransitionException;
import io.grove.quorumbreaker.cluster.ClusterCoordinator;
import io.grove.quorumbreaker.cluster.SentryClusterCoordinator;
import io.grove.quorumbreaker.gate.QuorumBreakerGate;
import lombok.extern.slf4j.Slf4j;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static io.github.resilience4j.circuitbreaker.CircuitBreaker.State.CLOSED;
import static io.github.resilience4j.circuitbreaker.CircuitBreaker.State.OPEN;

@Slf4j
public final class QuorumBreakerRegistryBinder implements AutoCloseable {

    public static final Duration DEFAULT_WINDOW_TTL_PADDING = Duration.ofSeconds(3);

    private static final Duration RESUBSCRIBE_CHECK_INTERVAL = Duration.ofSeconds(30);

    private final CircuitBreakerRegistry registry;
    private final ClusterCoordinator cluster;
    private final CircuitBreaker clusterCoordinatorGuard;
    private final Clock clock;
    private final Duration windowTtlPadding;

    private final Map<String, AutoCloseable> subscriptions = new ConcurrentHashMap<>();
    private final Set<String> boundNames = ConcurrentHashMap.newKeySet();
    private final Map<String, PendingSubscription> pendingSubscriptions = new ConcurrentHashMap<>();
    private volatile boolean closed = false;

    private record PendingSubscription(CircuitBreaker cb, QuorumBreakerGate gate) {
    }

    public QuorumBreakerRegistryBinder(CircuitBreakerRegistry registry,
                                        ClusterCoordinator cluster, Clock clock) {
        this(registry, cluster, clock, DEFAULT_WINDOW_TTL_PADDING);
    }

    public QuorumBreakerRegistryBinder(CircuitBreakerRegistry registry, ClusterCoordinator cluster,
                                        Clock clock, Duration windowTtlPadding) {
        if (windowTtlPadding.isNegative()) {
            throw new IllegalArgumentException("windowTtlPadding must not be negative: " + windowTtlPadding);
        }
        this.registry = registry;
        SentryClusterCoordinator sentry = asSentryCoordinator(cluster);
        this.cluster = sentry;
        this.clusterCoordinatorGuard = sentry.guard();
        this.clock = clock;
        this.windowTtlPadding = windowTtlPadding;
    }

    public Duration windowTtlPadding() {
        return windowTtlPadding;
    }

    public CircuitBreaker clusterCoordinatorGuard() {
        return clusterCoordinatorGuard;
    }

    public int boundCount() {
        return boundNames.size();
    }

    public Set<String> pendingResubscriptions() {
        return Set.copyOf(pendingSubscriptions.keySet());
    }

    public void bindAll() {
        registry.getEventPublisher().onEntryAdded(event -> CompletableFuture.runAsync(() -> bind(event.getAddedEntry()))
                .exceptionally(ex -> {
                    log.error("QuorumBreaker: failed to bind newly created circuit breaker '{}' - " +
                            "it will keep running as a plain, uncoordinated Resilience4j breaker",
                            event.getAddedEntry().getName(), ex);
                    return null;
                }));
        registry.getEventPublisher().onEntryRemoved(event -> unbind(event.getRemovedEntry()));
        registry.getAllCircuitBreakers().forEach(this::bind);
        scheduleResubscribeCheck();
    }

    @Override
    public void close() {
        closed = true;
        pendingSubscriptions.clear();
        subscriptions.values().forEach(QuorumBreakerRegistryBinder::closeQuietly);
        subscriptions.clear();
        boundNames.clear();
    }

    private static SentryClusterCoordinator asSentryCoordinator(ClusterCoordinator cluster) {
        return cluster instanceof SentryClusterCoordinator sentry
                ? sentry
                : new SentryClusterCoordinator(cluster);
    }

    private void bind(CircuitBreaker cb) {
        if (closed) return;
        if (cb instanceof QuorumBreaker) return;
        if (!boundNames.add(cb.getName())) return;

        CircuitBreakerConfig cfg = cb.getCircuitBreakerConfig();
        QuorumBreakerGate gate = new QuorumBreakerGate(cb.getName(), cluster,
                cfg.getPermittedNumberOfCallsInHalfOpenState(), cfg.getFailureRateThreshold(),
                cfg.getSlowCallRateThreshold(), cfg.getSlowCallDurationThreshold(),
                cfg.getWaitIntervalFunctionInOpenState(), windowTtlPadding, clock);

        cb.getEventPublisher().onStateTransition(event -> announceLocalTransition(gate,
                event.getStateTransition().getFromState(), event.getStateTransition().getToState()));
        attemptSubscription(cb, gate);

        if (closed) {
            boundNames.remove(cb.getName());
            return;
        }
        registry.replace(cb.getName(), new QuorumBreaker(cb, gate));
    }

    private void attemptSubscription(CircuitBreaker cb, QuorumBreakerGate gate) {
        String name = cb.getName();
        Optional<AutoCloseable> subscription = gate.onRemoteTransition(message -> applyRemoteTransition(cb, gate, message));
        if (subscription.isEmpty()) {
            if (!closed) pendingSubscriptions.put(name, new PendingSubscription(cb, gate));
            return;
        }
        pendingSubscriptions.remove(name);
        if (!closed && boundNames.contains(name)) {
            subscriptions.put(name, subscription.get());
        } else {
            closeQuietly(subscription.get());
        }
    }

    private void scheduleResubscribeCheck() {
        if (closed) return;
        CompletableFuture.delayedExecutor(RESUBSCRIBE_CHECK_INTERVAL.toMillis(), TimeUnit.MILLISECONDS)
                .execute(this::retryPendingSubscriptions);
    }

    private void retryPendingSubscriptions() {
        pendingSubscriptions.forEach((name, pending) -> {
            if (boundNames.contains(name)) {
                attemptSubscription(pending.cb(), pending.gate());
            } else {
                pendingSubscriptions.remove(name);
            }
        });
        scheduleResubscribeCheck();
    }

    private static void closeQuietly(AutoCloseable subscription) {
        try {
            subscription.close();
        } catch (Exception ignored) {
            // ignore
        }
    }

    private void announceLocalTransition(QuorumBreakerGate gate, CircuitBreaker.State fromState, CircuitBreaker.State toState) {
        if (fromState == CLOSED && toState == OPEN) {
            gate.announceOpen();
        }
    }

    private void applyRemoteTransition(CircuitBreaker cb, QuorumBreakerGate gate, String message) {
        try {
            if (message.startsWith(QuorumBreakerGate.OPEN_MESSAGE_PREFIX)) {
                applyRemoteOpen(cb, gate, message);
            } else if (CLOSED.name().equals(message) && cb.getState() != CLOSED) {
                transitionQuietly(cb, cb::transitionToClosedState);
            }
        } catch (RuntimeException malformed) {
            log.warn("QuorumBreaker[{}]: ignoring unparseable remote transition message '{}'",
                    cb.getName(), message, malformed);
        }
    }

    private void applyRemoteOpen(CircuitBreaker cb, QuorumBreakerGate gate, String message) {
        int count = Integer.parseInt(message.substring(QuorumBreakerGate.OPEN_MESSAGE_PREFIX.length()));
        gate.syncOpenCount(count);
        if (cb.getState() != OPEN) transitionQuietly(cb, cb::transitionToOpenState);
    }

    private void transitionQuietly(CircuitBreaker cb, Runnable transition) {
        try {
            transition.run();
        } catch (IllegalStateTransitionException alreadyResolved) {
            log.debug("QuorumBreaker[{}]: lost a race applying a remote transition - already at {}",
                    cb.getName(), cb.getState());
        }
    }

    private void unbind(CircuitBreaker cb) {
        boundNames.remove(cb.getName());
        pendingSubscriptions.remove(cb.getName());
        AutoCloseable subscription = subscriptions.remove(cb.getName());
        if (subscription == null) return;
        try {
            subscription.close();
        } catch (Exception e) {
            log.debug("QuorumBreaker[{}]: failed to close remote-transition subscription " +
                    "(topic/instance may already be gone)", cb.getName(), e);
        }
    }

}
