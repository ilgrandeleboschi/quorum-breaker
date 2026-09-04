package io.grove.quorumbreaker.cluster;

import io.grove.quorumbreaker.gate.ClaimParams;
import io.grove.quorumbreaker.gate.WindowOutcome;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static io.github.resilience4j.circuitbreaker.CircuitBreaker.State.CLOSED;

public final class InMemoryClusterCoordinator implements ClusterCoordinator {

    private final Clock clock;
    private final Map<String, Window> windows = new ConcurrentHashMap<>();
    private final Map<String, Set<Consumer<String>>> subscribers = new ConcurrentHashMap<>();
    private final AtomicInteger activeSubscriptions = new AtomicInteger();

    public InMemoryClusterCoordinator() {
        this(Clock.systemUTC());
    }

    public InMemoryClusterCoordinator(Clock clock) {
        this.clock = clock;
    }

    public int activeSubscriptions() {
        return activeSubscriptions.get();
    }

    @Override
    public CompletionStage<Void> publish(String channel, String message) {
        subscribers.getOrDefault(channel, Set.of()).forEach(listener -> listener.accept(message));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<AutoCloseable> subscribe(String channel, Consumer<String> listener) {
        Set<Consumer<String>> listeners = subscribers.computeIfAbsent(channel, c -> new CopyOnWriteArraySet<>());
        listeners.add(listener);
        activeSubscriptions.incrementAndGet();
        AutoCloseable subscription = () -> {
            if (listeners.remove(listener)) activeSubscriptions.decrementAndGet();
        };
        return CompletableFuture.completedFuture(subscription);
    }

    @Override
    public synchronized CompletionStage<Boolean> claim(String key, ClaimParams params, Duration ttl) {
        Window window = current(key);
        if (window == null) {
            window = new Window(params.permitted(), params.failureRateThreshold(), params.slowCallRateThreshold());
            windows.put(key, window);
        }
        boolean claimed = window.claimSlot();
        if (claimed) window.expiresAt = Instant.now(clock).plus(ttl);
        return CompletableFuture.completedFuture(claimed);
    }

    @Override
    public synchronized CompletionStage<WindowOutcome> trace(String key, ClaimParams params, boolean success, boolean slow, Duration ttl) {
        Window window = current(key);
        if (window == null) return CompletableFuture.completedFuture(WindowOutcome.EXPIRED);

        window.recordOutcome(success, slow);
        window.expiresAt = Instant.now(clock).plus(ttl);
        if (!window.markDecidedIfReady()) return CompletableFuture.completedFuture(WindowOutcome.PENDING);

        windows.remove(key);
        WindowOutcome decision = window.shouldClose() ? WindowOutcome.CLOSE : WindowOutcome.REOPEN;
        if (decision == WindowOutcome.CLOSE) publish(key, CLOSED.name());
        return CompletableFuture.completedFuture(decision);
    }

    private Window current(String key) {
        Window window = windows.get(key);
        if (window == null) return null;
        if (Instant.now(clock).isAfter(window.expiresAt)) {
            windows.remove(key);
            return null;
        }
        return window;
    }

    private static final class Window {
        private final int permitted;
        private final float failureRateThreshold;
        private final float slowCallRateThreshold;
        private int claimed;
        private int success;
        private int fail;
        private int slow;
        private boolean decided;
        private Instant expiresAt = Instant.MAX;

        Window(int permitted, float failureRateThreshold, float slowCallRateThreshold) {
            this.permitted = permitted;
            this.failureRateThreshold = failureRateThreshold;
            this.slowCallRateThreshold = slowCallRateThreshold;
        }

        boolean claimSlot() {
            if (claimed >= permitted) return false;
            claimed++;
            return true;
        }

        void recordOutcome(boolean success, boolean slow) {
            if (success) this.success++;
            else this.fail++;
            if (slow) this.slow++;
        }

        boolean markDecidedIfReady() {
            if (decided || success + fail < permitted) return false;
            decided = true;
            return true;
        }

        boolean shouldClose() {
            int total = success + fail;
            if (total == 0) return false;
            boolean failureRateOk = (fail * 100.0) / total < failureRateThreshold;
            boolean slowCallRateOk = (slow * 100.0) / total < slowCallRateThreshold;
            return failureRateOk && slowCallRateOk;
        }
    }

}
