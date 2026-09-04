package io.grove.quorumbreaker.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.grove.quorumbreaker.breaker.QuorumBreakerRegistryBinder;
import io.grove.quorumbreaker.testsupport.MutableClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.resilience4j.circuitbreaker.CircuitBreaker.State.CLOSED;
import static io.github.resilience4j.circuitbreaker.CircuitBreaker.State.OPEN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QuorumBreakerClusterIntegrationTest {

    private static final AtomicInteger PORT = new AtomicInteger(16001);
    private static final String BREAKER_NAME = "shared-service";
    private static final int PERMITTED_IN_HALF_OPEN = 4;
    private static final Duration WAIT_DURATION_IN_OPEN = Duration.ofMillis(300);

    private HazelcastInstance memberA;
    private HazelcastInstance memberB;

    @AfterEach
    void tearDown() {
        if (memberA != null) memberA.shutdown();
        if (memberB != null) memberB.shutdown();
    }

    private static CountDownLatch latchOnStateReached(CircuitBreaker cb, CircuitBreaker.State target) {
        CountDownLatch latch = new CountDownLatch(1);
        cb.getEventPublisher().onStateTransition(event -> {
            if (event.getStateTransition().getToState() == target) latch.countDown();
        });
        return latch;
    }

    private static final Duration ASYNC_WAIT = Duration.ofSeconds(20);

    private static void awaitLatch(CountDownLatch latch, String message) throws InterruptedException {
        assertTrue(latch.await(ASYNC_WAIT.toSeconds(), TimeUnit.SECONDS), message);
    }

    private static CompletionStage<String> awaitExactlyOneDenied(List<CompletionStage<String>> admissions)
            throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<CompletionStage<String>> firstDenied = new CompletableFuture<>();
        admissions.forEach(admission -> admission.handle((result, error) -> {
            if (error != null) firstDenied.complete(admission);
            return null;
        }));
        CompletionStage<String> denied = firstDenied.get(ASYNC_WAIT.toSeconds(), TimeUnit.SECONDS);

        List<CompletionStage<String>> allDenied = admissions.stream()
                .filter(admission -> admission.toCompletableFuture().isCompletedExceptionally())
                .toList();
        assertEquals(1, allDenied.size(), "expected exactly one of the concurrent attempts to be denied");
        return denied;
    }

    @Test
    void halfOpenBudget_isSharedAcrossTwoInstances_notDoubledPerInstance() throws Throwable {
        int portA = PORT.incrementAndGet();
        int portB = PORT.incrementAndGet();
        String clusterName = "quorum-breaker-itest-" + UUID.randomUUID();
        memberA = startEmbedded(clusterName, portA, List.of(portA, portB));
        memberB = startEmbedded(clusterName, portB, List.of(portA, portB));
        awaitClusterSize(memberA, 2);

        CircuitBreakerConfig cfg = CircuitBreakerConfig.custom()
                .slidingWindowSize(100)
                .minimumNumberOfCalls(100)
                .failureRateThreshold(50f)
                .waitDurationInOpenState(WAIT_DURATION_IN_OPEN)
                .permittedNumberOfCallsInHalfOpenState(PERMITTED_IN_HALF_OPEN)
                .build();

        MutableClock clock = new MutableClock(Instant.EPOCH);

        CircuitBreakerRegistry registryA = CircuitBreakerRegistry.of(cfg);
        registryA.circuitBreaker(BREAKER_NAME);
        new QuorumBreakerRegistryBinder(registryA, HazelcastClusterCoordinator.wrap(memberA, "itest"), clock).bindAll();
        CircuitBreaker cbA = registryA.circuitBreaker(BREAKER_NAME);

        CircuitBreakerRegistry registryB = CircuitBreakerRegistry.of(cfg);
        registryB.circuitBreaker(BREAKER_NAME);
        new QuorumBreakerRegistryBinder(registryB, HazelcastClusterCoordinator.wrap(memberB, "itest"), clock).bindAll();
        CircuitBreaker cbB = registryB.circuitBreaker(BREAKER_NAME);

        CountDownLatch bOpened = latchOnStateReached(cbB, OPEN);
        cbA.transitionToOpenState();
        assertEquals(OPEN, cbA.getState());
        awaitLatch(bOpened, "node B never learned that the breaker opened on node A");

        clock.advance(WAIT_DURATION_IN_OPEN.plusMillis(1));

        CountDownLatch admitted = new CountDownLatch(PERMITTED_IN_HALF_OPEN);
        List<CompletableFuture<String>> inFlight = new ArrayList<>();
        List<CompletionStage<String>> admissions = new ArrayList<>();
        for (int i = 0; i < PERMITTED_IN_HALF_OPEN / 2; i++) {
            CompletableFuture<String> call = new CompletableFuture<>();
            inFlight.add(call);
            admissions.add(cbA.executeCompletionStage(() -> {
                admitted.countDown();
                return call;
            }));
        }
        for (int i = 0; i < PERMITTED_IN_HALF_OPEN / 2; i++) {
            CompletableFuture<String> call = new CompletableFuture<>();
            inFlight.add(call);
            admissions.add(cbB.executeCompletionStage(() -> {
                admitted.countDown();
                return call;
            }));
        }
        assertEquals(PERMITTED_IN_HALF_OPEN, inFlight.size());
        awaitLatch(admitted, "not all trial calls were admitted (and their outcome callback registered) " +
                "before completing them");

        admissions.add(cbA.executeCompletionStage(() -> CompletableFuture.completedFuture("should-be-denied")));
        CompletionStage<String> denied = awaitExactlyOneDenied(admissions);
        ExecutionException rejected = assertThrows(ExecutionException.class, () -> denied.toCompletableFuture().get());
        assertInstanceOf(CallNotPermittedException.class, rejected.getCause());

        CountDownLatch aClosed = latchOnStateReached(cbA, CLOSED);
        CountDownLatch bClosed = latchOnStateReached(cbB, CLOSED);
        inFlight.forEach(call -> call.complete("ok"));

        awaitLatch(aClosed, "node A never closed after the shared window decided CLOSE");
        awaitLatch(bClosed, "node B never closed after the shared window decided CLOSE");
    }

    private static HazelcastInstance startEmbedded(String clusterName, int port, List<Integer> memberPorts) {
        Config config = new Config();
        config.setClusterName(clusterName);
        config.setProperty("hazelcast.logging.type", "none");

        NetworkConfig network = config.getNetworkConfig();
        network.setPort(port);
        network.setPortAutoIncrement(false);

        JoinConfig join = network.getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true);
        memberPorts.forEach(p -> join.getTcpIpConfig().addMember("127.0.0.1:" + p));

        return Hazelcast.newHazelcastInstance(config);
    }

    private static void awaitClusterSize(HazelcastInstance instance, int expectedSize) {
        if (instance.getCluster().getMembers().size() >= expectedSize) return;

        CountDownLatch reached = new CountDownLatch(1);
        com.hazelcast.cluster.MembershipListener listener = new com.hazelcast.cluster.MembershipListener() {
            @Override
            public void memberAdded(com.hazelcast.cluster.MembershipEvent event) {
                if (event.getMembers().size() >= expectedSize) reached.countDown();
            }

            @Override
            public void memberRemoved(com.hazelcast.cluster.MembershipEvent event) {
                // dummy
            }
        };

        var registration = instance.getCluster().addMembershipListener(listener);
        try {
            if (instance.getCluster().getMembers().size() >= expectedSize) return;
            if (!reached.await(15, TimeUnit.SECONDS))
                throw new AssertionError("Cluster never reached size " + expectedSize);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            instance.getCluster().removeMembershipListener(registration);
        }
    }

}
