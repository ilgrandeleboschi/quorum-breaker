package io.grove.quorumbreaker.gate;

import io.github.resilience4j.core.IntervalFunction;
import io.grove.quorumbreaker.cluster.InMemoryClusterCoordinator;
import io.grove.quorumbreaker.cluster.UnavailableClusterCoordinator;
import io.grove.quorumbreaker.testsupport.MutableClock;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QuorumBreakerGateTest {

    private final InMemoryClusterCoordinator cluster = new InMemoryClusterCoordinator();

    private static String breakerId() {
        return "breaker:" + UUID.randomUUID();
    }

    private QuorumBreakerGate gate(String breakerId, int permittedCalls, float failureRateThreshold) {
        return new QuorumBreakerGate(breakerId, cluster, permittedCalls, failureRateThreshold, 100f,
                Duration.ofSeconds(60), IntervalFunction.of(Duration.ofSeconds(10)), Duration.ofSeconds(10),
                Clock.systemUTC());
    }

    private QuorumBreakerGate gate(String breakerId, int permittedCalls, float failureRateThreshold,
                                    Duration waitDuration, Clock clock) {
        return new QuorumBreakerGate(breakerId, cluster, permittedCalls, failureRateThreshold, 100f,
                Duration.ofSeconds(60), IntervalFunction.of(waitDuration), Duration.ofSeconds(10), clock);
    }

    @Test
    void announceOpen_publishesToSubscribers() throws Exception {
        QuorumBreakerGate gate = gate(breakerId(), 3, 50f);
        AtomicReference<String> received = new AtomicReference<>();

        try (AutoCloseable ignored = gate.onRemoteTransition(received::set).orElseThrow()) {
            gate.announceOpen();
            assertEquals(QuorumBreakerGate.OPEN_MESSAGE_PREFIX + "1", received.get());
        }
    }

    @Test
    void requireAdmission_deniesImmediatelyAfterOpen_untilWaitDurationElapses() {
        MutableClock clock = new MutableClock(Instant.EPOCH);
        QuorumBreakerGate gate = gate(breakerId(), 3, 50f, Duration.ofSeconds(5), clock);

        gate.announceOpen();
        assertEquals(WindowAdmission.COOLING_DOWN, gate.requireAdmission().toCompletableFuture().join(),
                "must not probe the recovering dependency before waitDurationInOpenState has elapsed");

        clock.advance(Duration.ofSeconds(5));
        assertEquals(WindowAdmission.GRANTED, gate.requireAdmission().toCompletableFuture().join());
    }

    @Test
    void announceOpen_calledAgain_restartsCooldown() {
        MutableClock clock = new MutableClock(Instant.EPOCH);
        QuorumBreakerGate gate = gate(breakerId(), 3, 50f, Duration.ofSeconds(5), clock);

        gate.announceOpen();
        clock.advance(Duration.ofSeconds(4));
        gate.announceOpen();

        clock.advance(Duration.ofSeconds(4));
        assertEquals(WindowAdmission.COOLING_DOWN, gate.requireAdmission().toCompletableFuture().join(), "only 4s elapsed since the restarted cooldown");

        clock.advance(Duration.ofSeconds(1));
        assertEquals(WindowAdmission.GRANTED, gate.requireAdmission().toCompletableFuture().join());
    }

    @Test
    void reportOutcome_reopen_incrementsOpenCount_andAnnounceOpenBroadcastsIt() throws Exception {
        QuorumBreakerGate gate = gate(breakerId(), 1, 50f);
        AtomicReference<String> received = new AtomicReference<>();

        try (AutoCloseable ignored = gate.onRemoteTransition(received::set).orElseThrow()) {
            gate.requireAdmission().toCompletableFuture().join();
            assertEquals(WindowOutcome.REOPEN, gate.reportOutcome(false, false).toCompletableFuture().join());

            gate.announceOpen();
            assertEquals(QuorumBreakerGate.OPEN_MESSAGE_PREFIX + "2", received.get(),
                    "the attempt count must be incremented before the next OPEN broadcast");
        }
    }

    @Test
    void announceClose_resetsOpenCountBackToOne() throws Exception {
        QuorumBreakerGate gate = gate(breakerId(), 1, 50f);
        AtomicReference<String> received = new AtomicReference<>();

        gate.requireAdmission().toCompletableFuture().join();
        gate.reportOutcome(false, false).toCompletableFuture().join();
        gate.announceClose();

        try (AutoCloseable ignored = gate.onRemoteTransition(received::set).orElseThrow()) {
            gate.announceOpen();
            assertEquals(QuorumBreakerGate.OPEN_MESSAGE_PREFIX + "1", received.get(),
                    "a fresh incident must not inherit the previous incident's attempt count");
        }
    }

    @Test
    void syncOpenCount_updatesCountAndRestartsCooldown_evenWithoutALocalStateTransition() {
        MutableClock clock = new MutableClock(Instant.EPOCH);
        QuorumBreakerGate gate = gate(breakerId(), 3, 50f, Duration.ofSeconds(5), clock);

        gate.announceOpen();
        clock.advance(Duration.ofSeconds(3));
        gate.syncOpenCount(2);

        clock.advance(Duration.ofSeconds(3));
        assertEquals(WindowAdmission.COOLING_DOWN, gate.requireAdmission().toCompletableFuture().join(),
                "only 3s elapsed since syncOpenCount restarted the cooldown");

        clock.advance(Duration.ofSeconds(2));
        assertEquals(WindowAdmission.GRANTED, gate.requireAdmission().toCompletableFuture().join());
    }

    @Test
    void requireAdmission_startsOneAndClaimsASlot() {
        QuorumBreakerGate gate = gate(breakerId(), 3, 50f);

        assertEquals(WindowAdmission.GRANTED, gate.requireAdmission().toCompletableFuture().join());
    }

    @Test
    void requireAdmission_untilSlotsRunOut() {
        QuorumBreakerGate gate = gate(breakerId(), 2, 50f);

        assertEquals(WindowAdmission.GRANTED, gate.requireAdmission().toCompletableFuture().join());
        assertEquals(WindowAdmission.GRANTED, gate.requireAdmission().toCompletableFuture().join());
        assertEquals(WindowAdmission.DENIED, gate.requireAdmission().toCompletableFuture().join(), "only 2 slots were permitted");
    }

    @Test
    void requireAdmission_clusterThrows_returnsUnavailable() {
        QuorumBreakerGate gate = new QuorumBreakerGate(breakerId(), new UnavailableClusterCoordinator(), 3, 50f, 100f,
                Duration.ofSeconds(60), IntervalFunction.of(Duration.ofSeconds(10)), Duration.ofSeconds(10),
                Clock.systemUTC());

        assertEquals(WindowAdmission.UNAVAILABLE, gate.requireAdmission().toCompletableFuture().join());
        assertEquals(WindowOutcome.PENDING, gate.reportOutcome(true, false).toCompletableFuture().join());
    }

    @Test
    void announceOpen_clusterThrows_doesNotPropagate_andStillStartsTheLocalCooldown() {
        MutableClock clock = new MutableClock(Instant.EPOCH);
        QuorumBreakerGate gate = new QuorumBreakerGate(breakerId(), new UnavailableClusterCoordinator(), 3, 50f, 100f,
                Duration.ofSeconds(60), IntervalFunction.of(Duration.ofSeconds(5)), Duration.ofSeconds(10), clock);

        gate.announceOpen();

        assertEquals(WindowAdmission.COOLING_DOWN, gate.requireAdmission().toCompletableFuture().join());
        clock.advance(Duration.ofSeconds(5));
        assertEquals(WindowAdmission.UNAVAILABLE, gate.requireAdmission().toCompletableFuture().join(),
                "cooldown elapsed, so it should now reach the cluster (and fail there)");
    }

    @Test
    void onRemoteTransition_clusterThrows_returnsEmpty_insteadOfPropagating() {
        QuorumBreakerGate gate = new QuorumBreakerGate(breakerId(), new UnavailableClusterCoordinator(), 3, 50f, 100f,
                Duration.ofSeconds(60), IntervalFunction.of(Duration.ofSeconds(10)), Duration.ofSeconds(10),
                Clock.systemUTC());

        assertTrue(gate.onRemoteTransition(message -> { }).isEmpty(),
                "a failed subscribe attempt must be reported, not silently swallowed into a no-op");
    }

    @Test
    void reportOutcome_isPendingUntilBudgetFullyResolved() {
        QuorumBreakerGate gate = gate(breakerId(), 3, 50f);

        gate.requireAdmission().toCompletableFuture().join();

        assertEquals(WindowOutcome.PENDING, gate.reportOutcome(true, false).toCompletableFuture().join(),
                "only 1 of 3 permitted calls resolved -- window should still be in flight");
    }

    @Test
    void reportOutcome_resolvesToClose_whenFailureRateBelowThreshold() {
        QuorumBreakerGate gate = gate(breakerId(), 2, 50f);

        gate.requireAdmission().toCompletableFuture().join();
        gate.requireAdmission().toCompletableFuture().join();

        assertEquals(WindowOutcome.PENDING, gate.reportOutcome(true, false).toCompletableFuture().join());
        assertEquals(WindowOutcome.CLOSE, gate.reportOutcome(true, false).toCompletableFuture().join());
    }

    @Test
    void reportOutcome_resolvesToReopen_whenFailureRateAtOrAboveThreshold() {
        QuorumBreakerGate gate = gate(breakerId(), 2, 50f);

        gate.requireAdmission().toCompletableFuture().join();
        gate.requireAdmission().toCompletableFuture().join();

        assertEquals(WindowOutcome.PENDING, gate.reportOutcome(false, false).toCompletableFuture().join());
        assertEquals(WindowOutcome.REOPEN, gate.reportOutcome(false, false).toCompletableFuture().join());
    }

    @Test
    void reportOutcome_exactlyAtThreshold_stillReopens() {
        QuorumBreakerGate gate = gate(breakerId(), 2, 50f);

        gate.requireAdmission().toCompletableFuture().join();
        gate.requireAdmission().toCompletableFuture().join();

        assertEquals(WindowOutcome.PENDING, gate.reportOutcome(true, false).toCompletableFuture().join());
        assertEquals(WindowOutcome.REOPEN, gate.reportOutcome(false, false).toCompletableFuture().join());
    }

    @Test
    void concurrentRequireAdmission_neverGrantsMoreSlotsThanPermittedAcrossManyContenders() throws InterruptedException {
        int permitted = 5;
        int contenders = 50;
        QuorumBreakerGate gate = gate(breakerId(), permitted, 50f);

        CountDownLatch ready = new CountDownLatch(contenders);
        CountDownLatch go = new CountDownLatch(1);
        AtomicInteger slotsGranted = new AtomicInteger();
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < contenders; i++) {
            Thread t = new Thread(() -> {
                ready.countDown();
                try {
                    go.await();
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
                if (gate.requireAdmission().toCompletableFuture().join() == WindowAdmission.GRANTED) slotsGranted.incrementAndGet();
            });
            threads.add(t);
            t.start();
        }

        ready.await();
        go.countDown();
        for (Thread t : threads) t.join(5000);

        assertEquals(permitted, slotsGranted.get(),
                "exactly " + permitted + " of " + contenders + " concurrent recovery attempts should be granted a window slot");
        assertTrue(threads.stream().noneMatch(Thread::isAlive));
    }

}
