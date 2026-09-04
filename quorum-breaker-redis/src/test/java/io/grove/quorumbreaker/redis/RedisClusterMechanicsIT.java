package io.grove.quorumbreaker.redis;

import io.grove.quorumbreaker.gate.ClaimParams;
import io.grove.quorumbreaker.gate.WindowOutcome;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class RedisClusterMechanicsIT {

    @Container
    private static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);

    private RedisURI uri;
    private RedisClusterCoordinator coordinator;

    @BeforeEach
    void setUp() {
        uri = RedisURI.create(REDIS.getHost(), REDIS.getMappedPort(6379));
        coordinator = RedisClusterCoordinator.client(uri, "mechanics-it");
    }

    @AfterEach
    void tearDown() {
        coordinator.close();
    }

    @Test
    void claimAndTrace_useTheRealCasLuaScript_notJustMockedMath() throws Exception {
        String key = "breaker-" + UUID.randomUUID();
        ClaimParams params = new ClaimParams(2, 50f, 50f);
        Duration ttl = Duration.ofSeconds(5);

        assertTrue(claim(key, params, ttl));
        assertTrue(claim(key, params, ttl));
        assertFalse(claim(key, params, ttl), "a third claim must be denied once the budget is exhausted");

        assertEquals(WindowOutcome.PENDING, trace(key, params, true, false, ttl));
        assertEquals(WindowOutcome.CLOSE, trace(key, params, true, false, ttl));
    }

    @Test
    void concurrentClaims_neverExceedPermittedSlots() throws Exception {
        String key = "breaker-" + UUID.randomUUID();
        int slots = 10;
        int contenders = 50;
        ClaimParams params = new ClaimParams(slots, 50f, 100f);
        Duration ttl = Duration.ofSeconds(30);

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
                try {
                    if (claim(key, params, ttl)) granted.incrementAndGet();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            threads[i].start();
        }

        ready.await();
        go.countDown();
        for (Thread t : threads) t.join(10_000);

        assertEquals(slots, granted.get(), "the real CAS retry loop must never let concurrent claims exceed the permitted budget");
    }

    @Test
    void decision_isAppliedExactlyOnce_evenWithConcurrentOutcomes() throws Exception {
        String key = "breaker-" + UUID.randomUUID();
        ClaimParams params = new ClaimParams(2, 50f, 100f);
        Duration ttl = Duration.ofSeconds(30);

        assertTrue(claim(key, params, ttl));
        assertTrue(claim(key, params, ttl));

        CountDownLatch go = new CountDownLatch(1);
        AtomicInteger decided = new AtomicInteger();
        Thread[] threads = new Thread[2];
        for (int i = 0; i < 2; i++) {
            threads[i] = new Thread(() -> {
                try {
                    go.await();
                    if (trace(key, params, true, false, ttl) != WindowOutcome.PENDING) decided.incrementAndGet();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            threads[i].start();
        }
        go.countDown();
        for (Thread t : threads) t.join(10_000);

        assertEquals(1, decided.get(), "exactly one caller should observe the decision, never zero or two");
    }

    @Test
    void trace_reportsReopen_whenFailureRateExceedsThreshold() throws Exception {
        String key = "breaker-" + UUID.randomUUID();
        ClaimParams params = new ClaimParams(2, 50f, 50f);
        Duration ttl = Duration.ofSeconds(5);

        assertTrue(claim(key, params, ttl));
        assertTrue(claim(key, params, ttl));

        assertEquals(WindowOutcome.PENDING, trace(key, params, false, false, ttl));
        assertEquals(WindowOutcome.REOPEN, trace(key, params, false, false, ttl));
    }

    @Test
    void window_selfExpiresViaTtl_whenNeverDecided() throws Exception {
        String key = "breaker-" + UUID.randomUUID();
        ClaimParams params = new ClaimParams(2, 50f, 50f);
        Duration ttl = Duration.ofMillis(300);
        String fullKey = RedisClusterCoordinator.windowKeyPrefix("mechanics-it") + key;

        assertTrue(claim(key, params, ttl));

        try (RedisClient rawClient = RedisClient.create(uri);
             StatefulRedisConnection<String, String> raw = rawClient.connect()) {
            await().atMost(Duration.ofSeconds(5))
                    .pollInterval(Duration.ofMillis(20))
                    .until(() -> raw.sync().exists(fullKey) == 0);
        }

        assertEquals(WindowOutcome.EXPIRED, trace(key, params, true, false, ttl));
    }

    @Test
    void publishSubscribe_roundTripsOverRealRedisPubSub() throws Exception {
        String channel = "breaker-" + UUID.randomUUID();
        CountDownLatch received = new CountDownLatch(1);
        AtomicReference<String> payload = new AtomicReference<>();

        try (RedisClusterCoordinator subscriber = RedisClusterCoordinator.client(uri, "mechanics-it")) {
            AutoCloseable subscription = subscriber.subscribe(channel, message -> {
                payload.set(message);
                received.countDown();
            }).toCompletableFuture().get(5, TimeUnit.SECONDS);

            coordinator.publish(channel, "OPEN:1").toCompletableFuture().get(5, TimeUnit.SECONDS);

            assertTrue(received.await(5, TimeUnit.SECONDS), "the subscriber never received the published message");
            assertEquals("OPEN:1", payload.get());
            subscription.close();
        }
    }

    @Test
    void casSet_recoversFromNoScript_againstARealServer() throws Exception {
        String key = "breaker-" + UUID.randomUUID();
        ClaimParams params = new ClaimParams(3, 50f, 50f);
        Duration ttl = Duration.ofSeconds(5);

        assertTrue(claim(key, params, ttl));

        try (RedisClient rawClient = RedisClient.create(uri);
             StatefulRedisConnection<String, String> raw = rawClient.connect()) {
            raw.sync().scriptFlush();
        }

        assertTrue(claim(key, params, ttl), "a claim against an existing window right after SCRIPT FLUSH " +
                "must still succeed via the EVALSHA -> eval fallback, not just against a mock");
    }

    private boolean claim(String key, ClaimParams params, Duration ttl) throws Exception {
        return coordinator.claim(key, params, ttl).toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    private WindowOutcome trace(String key, ClaimParams params, boolean success, boolean slow, Duration ttl) throws Exception {
        return coordinator.trace(key, params, success, slow, ttl).toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

}
