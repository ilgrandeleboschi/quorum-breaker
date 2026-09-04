package io.grove.quorumbreaker.redis;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.grove.quorumbreaker.breaker.QuorumBreakerRegistryBinder;
import io.grove.quorumbreaker.testsupport.MutableClock;
import io.lettuce.core.RedisURI;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

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

import static io.github.resilience4j.circuitbreaker.CircuitBreaker.State.CLOSED;
import static io.github.resilience4j.circuitbreaker.CircuitBreaker.State.OPEN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class QuorumBreakerRedisClusterIT {

    @Container
    private static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);

    private static final String BREAKER_NAME = "shared-service";
    private static final int PERMITTED_IN_HALF_OPEN = 4;
    private static final Duration WAIT_DURATION_IN_OPEN = Duration.ofMillis(300);
    private static final Duration ASYNC_WAIT = Duration.ofSeconds(20);

    private RedisClusterCoordinator coordinatorA;
    private RedisClusterCoordinator coordinatorB;
    private QuorumBreakerRegistryBinder binderA;
    private QuorumBreakerRegistryBinder binderB;

    @AfterEach
    void tearDown() {
        if (binderA != null) binderA.close();
        if (binderB != null) binderB.close();
        if (coordinatorA != null) coordinatorA.close();
        if (coordinatorB != null) coordinatorB.close();
    }

    @Test
    void halfOpenBudget_isSharedAcrossTwoInstances_notDoubledPerInstance() throws Throwable {
        RedisURI uri = RedisURI.create(REDIS.getHost(), REDIS.getMappedPort(6379));
        String namespace = "itest-" + UUID.randomUUID();

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
        coordinatorA = RedisClusterCoordinator.client(uri, namespace);
        binderA = new QuorumBreakerRegistryBinder(registryA, coordinatorA, clock);
        binderA.bindAll();
        CircuitBreaker cbA = registryA.circuitBreaker(BREAKER_NAME);

        CircuitBreakerRegistry registryB = CircuitBreakerRegistry.of(cfg);
        registryB.circuitBreaker(BREAKER_NAME);
        coordinatorB = RedisClusterCoordinator.client(uri, namespace);
        binderB = new QuorumBreakerRegistryBinder(registryB, coordinatorB, clock);
        binderB.bindAll();
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

    private static CountDownLatch latchOnStateReached(CircuitBreaker cb, CircuitBreaker.State target) {
        CountDownLatch latch = new CountDownLatch(1);
        cb.getEventPublisher().onStateTransition(event -> {
            if (event.getStateTransition().getToState() == target) latch.countDown();
        });
        return latch;
    }

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

}
