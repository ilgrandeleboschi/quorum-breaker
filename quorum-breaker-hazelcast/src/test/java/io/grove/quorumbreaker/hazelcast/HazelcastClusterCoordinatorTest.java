package io.grove.quorumbreaker.hazelcast;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.grove.quorumbreaker.gate.ClaimParams;
import io.grove.quorumbreaker.gate.WindowOutcome;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.github.resilience4j.circuitbreaker.CircuitBreaker.State.CLOSED;
import static io.github.resilience4j.circuitbreaker.CircuitBreaker.State.OPEN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HazelcastClusterCoordinatorTest {

    private static final String NAMESPACE = "quorum-breaker-test-namespace";
    private static final AtomicInteger PORT = new AtomicInteger(15901);

    private static HazelcastInstance memberA;
    private static HazelcastInstance memberB;
    private static HazelcastClusterCoordinator coordinatorA;
    private static HazelcastClusterCoordinator coordinatorB;
    private static HazelcastClusterCoordinator clientCoordinator;

    @BeforeAll
    static void startCluster() {
        int portA = PORT.incrementAndGet();
        int portB = PORT.incrementAndGet();
        String clusterName = "quorum-breaker-test-" + UUID.randomUUID();

        memberA = startEmbedded(clusterName, portA, List.of(portA, portB));
        memberB = startEmbedded(clusterName, portB, List.of(portA, portB));
        awaitClusterSize(memberA, 2);
        coordinatorA = HazelcastClusterCoordinator.wrap(memberA, NAMESPACE);
        coordinatorB = HazelcastClusterCoordinator.wrap(memberB, NAMESPACE);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName(clusterName);
        clientConfig.setProperty("hazelcast.logging.type", "none");
        clientConfig.getNetworkConfig().addAddress("127.0.0.1:" + portA);
        clientCoordinator = HazelcastClusterCoordinator.client(clientConfig, NAMESPACE);
    }

    @AfterAll
    static void stopCluster() {
        clientCoordinator.close();
        coordinatorA.close();
        coordinatorB.close();
        memberA.shutdown();
        memberB.shutdown();
    }

    private static String key() {
        return "test:" + UUID.randomUUID();
    }

    @Test
    void p2p_claimedSlot_isSharedAcrossMembers() {
        String key = key();
        Duration ttl = Duration.ofSeconds(30);
        ClaimParams params = new ClaimParams(1, 50f, 100f);

        assertTrue(coordinatorA.claim(key, params, ttl).toCompletableFuture().join(), "first claim from member A should succeed");
        assertFalse(coordinatorB.claim(key, params, ttl).toCompletableFuture().join(), "member B must see the slot already claimed by member A");
    }

    @Test
    void clientServer_claim_sharedWithMember() {
        String key = key();
        Duration ttl = Duration.ofSeconds(30);
        ClaimParams params = new ClaimParams(1, 50f, 100f);

        assertTrue(clientCoordinator.claim(key, params, ttl).toCompletableFuture().join(), "client claim should be visible cluster-wide");
        assertFalse(coordinatorA.claim(key, params, ttl).toCompletableFuture().join(), "the managed member must see the slot claimed by the thin client");
    }

    @Test
    void concurrentClaims_neverExceedPermittedSlots() throws InterruptedException {
        String key = key();
        int slots = 10;
        int contenders = 50;
        ClaimParams params = new ClaimParams(slots, 50f, 100f);

        CountDownLatch ready = new CountDownLatch(contenders);
        CountDownLatch go = new CountDownLatch(1);
        AtomicInteger granted = new AtomicInteger();
        Thread[] threads = new Thread[contenders];

        for (int i = 0; i < contenders; i++) {
            threads[i] = new Thread(() -> {
                ready.countDown();
                try {
                    go.await();
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
                if (coordinatorA.claim(key, params, Duration.ofSeconds(30)).toCompletableFuture().join()) granted.incrementAndGet();
            });
            threads[i].start();
        }

        ready.await();
        go.countDown();
        for (Thread t : threads) t.join(5000);

        assertEquals(slots, granted.get());
    }

    @Test
    void trace_belowPermittedCount_staysPending() {
        String key = key();
        ClaimParams params = new ClaimParams(2, 50f, 100f);
        coordinatorA.claim(key, params, Duration.ofSeconds(30)).toCompletableFuture().join();

        assertEquals(WindowOutcome.PENDING, coordinatorA.trace(key, params, true, false, Duration.ofSeconds(30)).toCompletableFuture().join());
    }

    @Test
    void trace_lowFailureRate_decidesClose_andPublishesClosedMessage() throws InterruptedException {
        String key = key();
        ClaimParams params = new ClaimParams(2, 50f, 100f);
        coordinatorA.claim(key, params, Duration.ofSeconds(30)).toCompletableFuture().join();
        coordinatorA.claim(key, params, Duration.ofSeconds(30)).toCompletableFuture().join();

        AtomicReference<String> received = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        try (AutoCloseable ignored = coordinatorB.subscribe(key, msg -> {
            received.set(msg);
            latch.countDown();
        }).toCompletableFuture().join()) {
            assertEquals(WindowOutcome.PENDING, coordinatorA.trace(key, params, true, false, Duration.ofSeconds(30)).toCompletableFuture().join());
            assertEquals(WindowOutcome.CLOSE, coordinatorA.trace(key, params, true, false, Duration.ofSeconds(30)).toCompletableFuture().join());

            assertTrue(latch.await(5, TimeUnit.SECONDS));
            assertEquals(CLOSED.name(), received.get());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void trace_highFailureRate_decidesReopen_withoutPublishingAnything() throws InterruptedException {
        String key = key();
        ClaimParams params = new ClaimParams(2, 50f, 100f);
        coordinatorA.claim(key, params, Duration.ofSeconds(30)).toCompletableFuture().join();
        coordinatorA.claim(key, params, Duration.ofSeconds(30)).toCompletableFuture().join();

        AtomicReference<String> received = new AtomicReference<>();
        CountDownLatch messageArrived = new CountDownLatch(1);
        try (AutoCloseable ignored = coordinatorB.subscribe(key, msg -> {
            received.set(msg);
            messageArrived.countDown();
        }).toCompletableFuture().join()) {
            assertEquals(WindowOutcome.PENDING, coordinatorA.trace(key, params, false, false, Duration.ofSeconds(30)).toCompletableFuture().join());
            assertEquals(WindowOutcome.REOPEN, coordinatorA.trace(key, params, false, false, Duration.ofSeconds(30)).toCompletableFuture().join());

            assertFalse(messageArrived.await(300, TimeUnit.MILLISECONDS),
                    "trace() must not publish anything for a REOPEN decision");
            assertNull(received.get());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void decision_isAppliedExactlyOnce_evenWithConcurrentOutcomes() throws InterruptedException {
        String key = key();
        ClaimParams params = new ClaimParams(2, 50f, 100f);
        coordinatorA.claim(key, params, Duration.ofSeconds(30)).toCompletableFuture().join();
        coordinatorA.claim(key, params, Duration.ofSeconds(30)).toCompletableFuture().join();

        CountDownLatch go = new CountDownLatch(1);
        AtomicInteger decided = new AtomicInteger();
        Thread[] threads = new Thread[2];
        for (int i = 0; i < 2; i++) {
            threads[i] = new Thread(() -> {
                try {
                    go.await();
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
                if (coordinatorA.trace(key, params, true, false, Duration.ofSeconds(30)).toCompletableFuture().join() != WindowOutcome.PENDING) {
                    decided.incrementAndGet();
                }
            });
            threads[i].start();
        }
        go.countDown();
        for (Thread t : threads) t.join(5000);

        assertEquals(1, decided.get(), "exactly one caller should observe the decision, never zero or two");
    }

    @Test
    void window_expiresOnItsOwn_insteadOfLeakingForever() {
        String key = key();
        coordinatorA.claim(key, new ClaimParams(1, 50f, 100f), Duration.ofSeconds(30)).toCompletableFuture().join();

        long expiry = memberA.<String, Object>getMap(HazelcastClusterCoordinator.windowsMapName(NAMESPACE)).getEntryView(key).getExpirationTime();

        assertTrue(expiry > System.currentTimeMillis() && expiry < Long.MAX_VALUE,
                "window entry must carry a finite expiration, or it leaks permanently if outcomes never report back");
    }

    @Test
    void claim_refreshesTtl_onExistingWindow() {
        String key = key();
        ClaimParams params = new ClaimParams(2, 50f, 100f);
        Duration ttl = Duration.ofSeconds(30);
        IMap<String, Object> windows = memberA.getMap(HazelcastClusterCoordinator.windowsMapName(NAMESPACE));

        coordinatorA.claim(key, params, ttl).toCompletableFuture().join();
        long expiryAfterFirstClaim = windows.getEntryView(key).getExpirationTime();

        coordinatorA.claim(key, params, ttl).toCompletableFuture().join();
        long expiryAfterSecondClaim = windows.getEntryView(key).getExpirationTime();

        assertTrue(expiryAfterSecondClaim >= expiryAfterFirstClaim,
                "a later claim on the same window must push out its TTL");
    }

    @Test
    void release_freesTheSlot_whenGenerationStillMatches() {
        String key = key();
        ClaimParams params = new ClaimParams(1, 50f, 100f);

        HazelcastClusterCoordinator.ClaimResult first = coordinatorA.claimOnce(key, params, 30_000);
        assertTrue(first.granted());
        assertFalse(coordinatorA.claim(key, params, Duration.ofSeconds(30)).toCompletableFuture().join(),
                "the slot is already fully claimed, a further claim must be denied");

        coordinatorA.release(key, first.generation());

        assertTrue(coordinatorA.claim(key, params, Duration.ofSeconds(30)).toCompletableFuture().join(),
                "releasing the slot back must make it claimable again");
    }

    @Test
    void release_isNoOp_forAStaleGenerationFromAnAlreadyReplacedWindow() {
        String key = key();
        ClaimParams params = new ClaimParams(1, 50f, 100f);

        HazelcastClusterCoordinator.ClaimResult stale = coordinatorA.claimOnce(key, params, 30_000);
        assertTrue(stale.granted());

        memberA.<String, String>getMap(HazelcastClusterCoordinator.windowsMapName(NAMESPACE)).delete(key);

        HazelcastClusterCoordinator.ClaimResult fresh = coordinatorA.claimOnce(key, params, 30_000);
        assertTrue(fresh.granted());
        assertNotEquals(stale.generation(), fresh.generation());

        coordinatorA.release(key, stale.generation());

        assertFalse(coordinatorA.claim(key, params, Duration.ofSeconds(30)).toCompletableFuture().join(),
                "a release carrying a stale generation must not free a slot that belongs to a newer window");
    }

    @Test
    void eventBus_deliversPublishedMessageToSubscriber() throws InterruptedException {
        String channel = key();
        AtomicReference<String> received = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        try (AutoCloseable ignored = coordinatorA.subscribe(channel, msg -> {
            received.set(msg);
            latch.countDown();
        }).toCompletableFuture().join()) {
            coordinatorA.publish(channel, OPEN.name());
            assertTrue(latch.await(5, TimeUnit.SECONDS));
            assertEquals(OPEN.name(), received.get());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void close_onWrappedInstance_doesNotShutDownTheUnderlyingMember() {
        HazelcastClusterCoordinator wrapping = HazelcastClusterCoordinator.wrap(memberB, NAMESPACE);

        wrapping.close();

        assertTrue(memberB.getLifecycleService().isRunning(), "wrap() does not own the instance, so close() must not shut it down");
    }

    @Test
    void wrap_blankNamespace_throws() {
        assertThrows(IllegalArgumentException.class, () -> HazelcastClusterCoordinator.wrap(memberA, " "));
    }

    @Test
    void wrap_withCustomExecutor_runsClusterCallsOnIt() {
        AtomicInteger tasksRun = new AtomicInteger();
        Executor countingExecutor = task -> {
            tasksRun.incrementAndGet();
            task.run();
        };
        HazelcastClusterCoordinator coordinator = HazelcastClusterCoordinator.wrap(
                memberA, NAMESPACE, Duration.ofSeconds(1), countingExecutor);

        assertTrue(coordinator.claim(key(), new ClaimParams(1, 50f, 100f), Duration.ofSeconds(30))
                .toCompletableFuture().join());

        assertTrue(tasksRun.get() > 0, "cluster calls must run on the supplied executor");
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
