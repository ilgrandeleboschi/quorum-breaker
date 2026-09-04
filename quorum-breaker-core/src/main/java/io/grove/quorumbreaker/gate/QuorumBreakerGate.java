package io.grove.quorumbreaker.gate;

import io.github.resilience4j.core.IntervalFunction;
import io.grove.quorumbreaker.cluster.ClusterCoordinator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static io.grove.quorumbreaker.gate.WindowAdmission.COOLING_DOWN;
import static io.grove.quorumbreaker.gate.WindowAdmission.DENIED;
import static io.grove.quorumbreaker.gate.WindowAdmission.GRANTED;
import static io.grove.quorumbreaker.gate.WindowAdmission.UNAVAILABLE;
import static io.grove.quorumbreaker.gate.WindowOutcome.EXPIRED;
import static io.grove.quorumbreaker.gate.WindowOutcome.PENDING;
import static io.grove.quorumbreaker.gate.WindowOutcome.REOPEN;

@Slf4j
@RequiredArgsConstructor
public final class QuorumBreakerGate {

    public static final String OPEN_MESSAGE_PREFIX = "OPEN:";

    private final String breakerId;
    private final ClusterCoordinator cluster;
    private final int permittedCallsInHalfOpen;
    private final float failureRateThreshold;
    private final float slowCallRateThreshold;
    private final Duration slowCallDurationThreshold;
    private final IntervalFunction waitIntervalFunction;
    private final Duration windowTtlPadding;
    private final Clock clock;

    private final AtomicReference<OpenWindow> openWindow = new AtomicReference<>(new OpenWindow(1, null));

    public void announceOpen() {
        OpenWindow current = openWindow.updateAndGet(w -> new OpenWindow(w.count(), clock.instant()));
        try {
            cluster.publish(breakerId, OPEN_MESSAGE_PREFIX + current.count())
                    .exceptionally(clusterUnavailable -> {
                        log.warn("QuorumBreaker[{}]: could not announce OPEN to the cluster",
                                breakerId, clusterUnavailable);
                        return null;
                    });
        } catch (RuntimeException clusterUnavailable) {
            log.warn("QuorumBreaker[{}]: could not announce OPEN to the cluster",
                    breakerId, clusterUnavailable);
        }
    }

    public void announceClose() {
        openWindow.updateAndGet(w -> new OpenWindow(1, w.since()));
    }

    public void syncOpenCount(int count) {
        openWindow.updateAndGet(w -> count > w.count() ? new OpenWindow(count, clock.instant()) : w);
    }

    public int openCount() {
        return openWindow.get().count();
    }

    public Optional<AutoCloseable> onRemoteTransition(Consumer<String> onMessage) {
        try {
            return Optional.of(cluster.subscribe(breakerId, onMessage).toCompletableFuture().join());
        } catch (RuntimeException clusterUnavailable) {
            log.warn("QuorumBreaker[{}]: could not subscribe to remote transitions",
                    breakerId, clusterUnavailable);
            return Optional.empty();
        }
    }

    public CompletionStage<WindowAdmission> requireAdmission() {
        if (stillCoolingDown()) return CompletableFuture.completedFuture(COOLING_DOWN);
        try {
            ClaimParams params = new ClaimParams(permittedCallsInHalfOpen, failureRateThreshold, slowCallRateThreshold);
            return cluster.claim(breakerId, params, windowTtl()).handle(this::toAdmission);
        } catch (RuntimeException clusterUnavailable) {
            logClusterUnavailable("claim a trial slot", clusterUnavailable);
            return CompletableFuture.completedFuture(UNAVAILABLE);
        }
    }

    private WindowAdmission toAdmission(Boolean claimed, Throwable clusterUnavailable) {
        if (clusterUnavailable != null) {
            logClusterUnavailable("claim a trial slot", clusterUnavailable);
            return UNAVAILABLE;
        }
        return Boolean.TRUE.equals(claimed) ? GRANTED : DENIED;
    }

    private boolean stillCoolingDown() {
        Instant since = openWindow.get().since();
        return since != null && Duration.between(since, clock.instant()).compareTo(waitDuration()) < 0;
    }

    public CompletionStage<WindowOutcome> reportOutcome(boolean success, boolean slow) {
        try {
            ClaimParams params = new ClaimParams(permittedCallsInHalfOpen, failureRateThreshold, slowCallRateThreshold);
            return cluster.trace(breakerId, params, success, slow, windowTtl()).handle((outcome, clusterUnavailable) -> {
                if (clusterUnavailable != null) {
                    logClusterUnavailable("report a trial outcome", clusterUnavailable);
                    return PENDING;
                }
                if (outcome == REOPEN) openWindow.updateAndGet(w -> new OpenWindow(w.count() + 1, w.since()));
                if (outcome == EXPIRED) {
                    log.warn("QuorumBreaker[{}]: a trial outcome arrived after its shared window had already " +
                            "expired - windowTtlPadding may be too small for this breaker's traffic; " +
                            "treating it as still pending", breakerId);
                    return PENDING;
                }
                return outcome;
            });
        } catch (RuntimeException clusterUnavailable) {
            logClusterUnavailable("report a trial outcome", clusterUnavailable);
            return CompletableFuture.completedFuture(PENDING);
        }
    }

    private void logClusterUnavailable(String action, Throwable cause) {
        log.warn("QuorumBreaker[{}]: could not reach the cluster to {}, falling back to a local decision",
                breakerId, action, cause);
    }

    private Duration windowTtl() {
        return slowCallDurationThreshold.plus(windowTtlPadding);
    }

    private Duration waitDuration() {
        return Duration.ofMillis(waitIntervalFunction.apply(openWindow.get().count()));
    }

    private record OpenWindow(int count, Instant since) {
    }

}
