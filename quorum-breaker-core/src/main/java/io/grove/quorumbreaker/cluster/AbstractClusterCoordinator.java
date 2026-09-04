package io.grove.quorumbreaker.cluster;

import io.grove.quorumbreaker.gate.ClaimParams;
import io.grove.quorumbreaker.gate.WindowOutcome;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.github.resilience4j.circuitbreaker.CircuitBreaker.State.CLOSED;
import static io.grove.quorumbreaker.gate.WindowOutcome.CLOSE;
import static io.grove.quorumbreaker.gate.WindowOutcome.REOPEN;

@Slf4j
public abstract class AbstractClusterCoordinator implements ClusterCoordinator, AutoCloseable {

    protected final Duration callTimeout;
    protected final Executor executor;

    public record ClaimResult(boolean granted, String generation) {
    }

    protected AbstractClusterCoordinator(Duration callTimeout, Executor executor) {
        this.callTimeout = callTimeout;
        this.executor = executor;
    }

    protected abstract ClaimResult claimOnce(String key, ClaimParams params, long ttlMillis);

    protected abstract void release(String key, String generation);

    protected abstract WindowOutcome traceOnce(String key, ClaimParams params, boolean success, boolean slow, long ttlMillis);

    protected abstract CompletionStage<Void> evict(String key);

    public static Executor defaultExecutor() {
        return Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("quorum-breaker-cluster-", 0).factory());
    }

    protected final CompletionStage<AutoCloseable> subscribeBounded(String channel, Supplier<AutoCloseable> register) {
        AtomicReference<Thread> runner = new AtomicReference<>();
        CompletableFuture<AutoCloseable> original = CompletableFuture.supplyAsync(() -> {
            runner.set(Thread.currentThread());
            try {
                return register.get();
            } finally {
                runner.set(null);
            }
        }, executor);
        CompletionStage<AutoCloseable> bounded = bounded(callTimeout, original);
        original.whenComplete((subscription, failure) -> {
            if (bounded.toCompletableFuture().isCompletedExceptionally() && subscription != null) {
                log.warn("QuorumBreaker[{}]: a subscription registered after this caller had already " +
                        "given up on it (cluster call exceeded callTimeout) - closing it immediately", channel);
                closeQuietly(subscription);
            }
        });
        bounded.toCompletableFuture().whenComplete((ignored, timedOut) -> {
            if (timedOut == null) return;
            Thread stillRunning = runner.get();
            if (stillRunning != null) stillRunning.interrupt();
        });
        return bounded;
    }

    private static void closeQuietly(AutoCloseable subscription) {
        try {
            subscription.close();
        } catch (Exception ignored) {
            // ignored
        }
    }

    @Override
    public final CompletionStage<Boolean> claim(String key, ClaimParams params, Duration ttl) {
        long ttlMillis = toMillis(ttl);
        CompletableFuture<ClaimResult> original = CompletableFuture.supplyAsync(() -> claimOnce(key, params, ttlMillis), executor);
        CompletionStage<ClaimResult> bounded = bounded(callTimeout, original);
        original.whenComplete((result, failure) -> {
            if (bounded.toCompletableFuture().isCompletedExceptionally()
                    && result != null && result.granted()) {
                log.warn("QuorumBreaker[{}]: a claim arrived after this caller had already given up on it " +
                        "(cluster call exceeded callTimeout) - releasing the trial slot back", key);
                try {
                    release(key, result.generation());
                } catch (RuntimeException releaseFailed) {
                    log.warn("QuorumBreaker[{}]: failed to release a timed-out claim back to the cluster - " +
                            "the trial slot will stay claimed until the window's TTL expires", key, releaseFailed);
                }
            }
        });
        return bounded.thenApply(ClaimResult::granted);
    }

    @Override
    public final CompletionStage<WindowOutcome> trace(String key, ClaimParams params, boolean success, boolean slow, Duration ttl) {
        long ttlMillis = toMillis(ttl);
        return bounded(callTimeout, CompletableFuture.supplyAsync(() -> traceOnce(key, params, success, slow, ttlMillis), executor))
                .thenApply(decision -> {
                    if (decision == CLOSE || decision == REOPEN) {
                        announceAndEvict(key, decision);
                    }
                    return decision;
                });
    }

    private void announceAndEvict(String key, WindowOutcome decision) {
        CompletionStage<Void> announced = decision == CLOSE
                ? publish(key, CLOSED.name()).exceptionally(broadcastFailed -> {
                    log.warn("QuorumBreaker[{}]: decided {} but failed to broadcast it to the cluster",
                            key, decision, broadcastFailed);
                    return null;
                })
                : CompletableFuture.completedFuture(null);

        announced.thenCompose(ignored -> bounded(callTimeout, evict(key))
                .exceptionally(evictFailed -> {
                    log.warn("QuorumBreaker[{}]: decided {} but failed to evict the shared window state; " +
                            "it will expire via TTL", key, decision, evictFailed);
                    return null;
                }));
    }

    public static <T> CompletionStage<T> bounded(Duration callTimeout, CompletionStage<T> stage) {
        return stage.toCompletableFuture()
                .thenApply(Function.identity())
                .orTimeout(callTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .handle((result, failure) -> unwrap(callTimeout, result, failure));
    }

    private static <T> T unwrap(Duration callTimeout, T result, Throwable failure) {
        if (failure == null) return result;
        Throwable cause = failure instanceof CompletionException || failure instanceof ExecutionException
                ? failure.getCause() : failure;
        if (cause instanceof TimeoutException) {
            throw new ClusterCoordinatorException("Cluster coordinator call timed out after " + callTimeout, cause);
        }
        if (cause instanceof RuntimeException re) {
            throw re;
        }
        throw new ClusterCoordinatorException("Cluster coordinator call failed", cause);
    }

    protected static long toMillis(Duration ttl) {
        if (ttl == null || ttl.isNegative()) throw new IllegalArgumentException("TTL must not be negative");
        return Math.max(1, ttl.toMillis());
    }

}
