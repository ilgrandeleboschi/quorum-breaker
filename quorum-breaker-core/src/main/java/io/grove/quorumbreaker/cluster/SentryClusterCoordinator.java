package io.grove.quorumbreaker.cluster;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.grove.quorumbreaker.gate.ClaimParams;
import io.grove.quorumbreaker.gate.WindowOutcome;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import static io.github.resilience4j.circuitbreaker.CircuitBreaker.State.CLOSED;
import static io.github.resilience4j.circuitbreaker.CircuitBreaker.State.OPEN;

@Slf4j
public final class SentryClusterCoordinator implements ClusterCoordinator {

    public static final String NAME = "quorum-breaker::cluster-coordinator";

    private static final CircuitBreakerConfig DEFAULT_CONFIG = CircuitBreakerConfig.custom()
            .slidingWindowSize(10)
            .minimumNumberOfCalls(5)
            .failureRateThreshold(50f)
            .waitDurationInOpenState(Duration.ofSeconds(2))
            .permittedNumberOfCallsInHalfOpenState(1)
            .build();

    private final ClusterCoordinator delegate;
    private final CircuitBreaker guard;

    public SentryClusterCoordinator(ClusterCoordinator delegate) {
        this(delegate, DEFAULT_CONFIG);
    }

    public SentryClusterCoordinator(ClusterCoordinator delegate, CircuitBreakerConfig config) {
        this.delegate = delegate;
        this.guard = CircuitBreaker.of(NAME, config);
        guard.getEventPublisher().onStateTransition(event -> {
            CircuitBreaker.State toState = event.getStateTransition().getToState();
            if (toState == OPEN) {
                log.warn("QuorumBreaker: cluster coordinator is failing, falling back to local circuit breaker behaviour");
            } else if (toState == CLOSED) {
                log.info("QuorumBreaker: cluster coordinator recovered, resuming cluster coordination");
            }
        });
    }

    public CircuitBreaker guard() {
        return guard;
    }

    @Override
    public CompletionStage<Void> publish(String channel, String message) {
        return guard.executeCompletionStage(() -> delegate.publish(channel, message));
    }

    @Override
    public CompletionStage<AutoCloseable> subscribe(String channel, Consumer<String> listener) {
        return guard.executeCompletionStage(() -> delegate.subscribe(channel, listener));
    }

    @Override
    public CompletionStage<Boolean> claim(String key, ClaimParams params, Duration ttl) {
        return guard.executeCompletionStage(() -> delegate.claim(key, params, ttl));
    }

    @Override
    public CompletionStage<WindowOutcome> trace(String key, ClaimParams params, boolean success, boolean slow, Duration ttl) {
        return guard.executeCompletionStage(() -> delegate.trace(key, params, success, slow, ttl));
    }

}
