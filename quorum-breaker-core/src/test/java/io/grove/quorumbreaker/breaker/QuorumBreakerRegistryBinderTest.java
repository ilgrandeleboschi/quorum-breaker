package io.grove.quorumbreaker.breaker;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.grove.quorumbreaker.cluster.ClusterCoordinator;
import io.grove.quorumbreaker.cluster.FlakySubscribeClusterCoordinator;
import io.grove.quorumbreaker.cluster.InMemoryClusterCoordinator;
import io.grove.quorumbreaker.cluster.SentryClusterCoordinator;
import io.grove.quorumbreaker.gate.QuorumBreakerGate;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.github.resilience4j.circuitbreaker.CircuitBreaker.State.CLOSED;
import static io.github.resilience4j.circuitbreaker.CircuitBreaker.State.OPEN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QuorumBreakerRegistryBinderTest {

    private static final CircuitBreakerConfig CONFIG = CircuitBreakerConfig.custom()
            .slidingWindowSize(2)
            .minimumNumberOfCalls(2)
            .failureRateThreshold(50f)
            .waitDurationInOpenState(Duration.ofSeconds(10))
            .permittedNumberOfCallsInHalfOpenState(3)
            .build();

    private final InMemoryClusterCoordinator cluster = new InMemoryClusterCoordinator();

    private static CircuitBreaker awaitBound(CircuitBreakerRegistry registry, String name) throws InterruptedException {
        CircuitBreaker existing = registry.circuitBreaker(name);
        if (existing instanceof QuorumBreaker) return existing;

        CountDownLatch bound = new CountDownLatch(1);
        registry.getEventPublisher().onEntryReplaced(event -> {
            if (event.getOldEntry().getName().equals(name)) bound.countDown();
        });

        if (registry.circuitBreaker(name) instanceof QuorumBreaker qb) return qb;
        assertTrue(bound.await(2, TimeUnit.SECONDS), name + " was never wrapped within the deadline");
        return registry.circuitBreaker(name);
    }

    private static void triggerResubscribeCheck(QuorumBreakerRegistryBinder binder) throws Exception {
        Method retry = QuorumBreakerRegistryBinder.class.getDeclaredMethod("retryPendingSubscriptions");
        retry.setAccessible(true);
        retry.invoke(binder);
    }

    @Test
    void bindAll_wrapsBreakersAlreadyInTheRegistry() {
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(CONFIG);
        registry.circuitBreaker("pre-existing");

        new QuorumBreakerRegistryBinder(registry, cluster, Clock.systemUTC()).bindAll();

        assertInstanceOf(QuorumBreaker.class, registry.circuitBreaker("pre-existing"));
    }

    @Test
    void bindAll_wrapsBreakersAddedAfterwards() throws InterruptedException {
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(CONFIG);
        new QuorumBreakerRegistryBinder(registry, cluster, Clock.systemUTC()).bindAll();

        awaitBound(registry, "added-later");
    }

    @Test
    void bindAll_doesNotDoubleWrapAnAlreadyWrappedBreaker() throws InterruptedException {
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(CONFIG);
        QuorumBreakerRegistryBinder binder = new QuorumBreakerRegistryBinder(registry, cluster, Clock.systemUTC());
        binder.bindAll();
        CircuitBreaker wrapped = awaitBound(registry, "only-wrapped-once");

        binder.bindAll();

        assertSame(registry.circuitBreaker("only-wrapped-once"), wrapped);
    }

    @Test
    void remoteOpenMessage_transitionsTheLocalBreakerToOpen() throws InterruptedException {
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(CONFIG);
        new QuorumBreakerRegistryBinder(registry, cluster, Clock.systemUTC()).bindAll();
        CircuitBreaker cb = awaitBound(registry, "remote-open");
        assertEquals(CLOSED, cb.getState());

        cluster.publish("remote-open", QuorumBreakerGate.OPEN_MESSAGE_PREFIX + "1");

        assertEquals(OPEN, cb.getState());
    }

    @Test
    void remoteClosedMessage_transitionsTheLocalBreakerToClosed() throws InterruptedException {
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(CONFIG);
        new QuorumBreakerRegistryBinder(registry, cluster, Clock.systemUTC()).bindAll();
        CircuitBreaker cb = awaitBound(registry, "remote-closed");
        cb.transitionToOpenState();
        cb.transitionToHalfOpenState();

        cluster.publish("remote-closed", CLOSED.name());

        assertEquals(CLOSED, cb.getState());
    }

    @Test
    void localOpenTransition_isAnnouncedToTheCluster() throws Exception {
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(CONFIG);
        new QuorumBreakerRegistryBinder(registry, cluster, Clock.systemUTC()).bindAll();
        CircuitBreaker cb = awaitBound(registry, "local-open");

        AtomicReference<String> received = new AtomicReference<>();
        try (AutoCloseable ignored = cluster.subscribe("local-open", received::set).toCompletableFuture().join()) {
            cb.transitionToOpenState();
            assertEquals(QuorumBreakerGate.OPEN_MESSAGE_PREFIX + "1", received.get());
        }
    }

    @Test
    void halfOpenAutoDecision_byResilience4jItself_isNeverAnnouncedToTheCluster() throws Exception {
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(CONFIG);
        CircuitBreaker raw = registry.circuitBreaker("silent-half-open");
        new QuorumBreakerRegistryBinder(registry, cluster, Clock.systemUTC()).bindAll();
        awaitBound(registry, "silent-half-open");

        List<String> received = Collections.synchronizedList(new ArrayList<>());
        try (AutoCloseable ignored = cluster.subscribe("silent-half-open", received::add).toCompletableFuture().join()) {
            raw.transitionToOpenState();
            raw.transitionToHalfOpenState();
            received.clear(); // drop the genuine CLOSED -> OPEN announce, only the next verdict matters here

            // permittedNumberOfCallsInHalfOpenState=3, failureRateThreshold=50f: 2 failures out of 3
            // trips Resilience4j's own half-open verdict back to OPEN, entirely on its own.
            raw.onError(0, TimeUnit.MILLISECONDS, new RuntimeException("boom"));
            raw.onError(0, TimeUnit.MILLISECONDS, new RuntimeException("boom"));
            raw.onSuccess(0, TimeUnit.MILLISECONDS);

            assertEquals(OPEN, raw.getState(), "Resilience4j should have decided on its own from this node's sample");
            assertTrue(received.isEmpty(), "a purely local half-open verdict must never be broadcast to the cluster");
        }
    }

    @Test
    void removingABreaker_closesItsClusterSubscription() throws InterruptedException {
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(CONFIG);
        new QuorumBreakerRegistryBinder(registry, cluster, Clock.systemUTC()).bindAll();
        awaitBound(registry, "leaky");
        assertEquals(1, cluster.activeSubscriptions());

        registry.remove("leaky");

        assertEquals(0, cluster.activeSubscriptions(),
                "the topic listener registered for this breaker must be unsubscribed, not leaked forever");
    }

    @Test
    void reAddingABreakerAfterRemoval_bindsAFreshSubscription() throws InterruptedException {
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(CONFIG);
        new QuorumBreakerRegistryBinder(registry, cluster, Clock.systemUTC()).bindAll();
        awaitBound(registry, "recycled");
        registry.remove("recycled");
        assertEquals(0, cluster.activeSubscriptions());

        CircuitBreaker recreated = awaitBound(registry, "recycled");

        assertInstanceOf(QuorumBreaker.class, recreated);
        assertEquals(1, cluster.activeSubscriptions());
    }

    @Test
    void bind_itself_usesReplace_whichMustNotTriggerItsOwnUnbind() throws InterruptedException {
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(CONFIG);
        new QuorumBreakerRegistryBinder(registry, cluster, Clock.systemUTC()).bindAll();

        awaitBound(registry, "replaced");

        assertEquals(1, cluster.activeSubscriptions());
    }

    @Test
    void bind_subscribeFailsInitially_breakerIsStillWrapped_butWithoutARealSubscription() throws InterruptedException {
        FlakySubscribeClusterCoordinator flaky = new FlakySubscribeClusterCoordinator();
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(CONFIG);
        QuorumBreakerRegistryBinder binder = new QuorumBreakerRegistryBinder(registry, flaky, Clock.systemUTC());
        binder.bindAll();

        CircuitBreaker cb = awaitBound(registry, "flaky-at-first");

        assertInstanceOf(QuorumBreaker.class, cb,
                "must still be wrapped and gate claim/trace even without remote awareness");
        assertEquals(0, flaky.activeSubscriptions(),
                "the subscribe attempt failed, so no real subscription should exist yet");
        assertEquals(1, binder.boundCount());
        assertEquals(Set.of("flaky-at-first"), binder.pendingResubscriptions(),
                "the breaker must be visible as pending until its subscription actually succeeds");
    }

    @Test
    void retryPendingSubscriptions_installsTheRealSubscription_onceTheClusterHeals() throws Exception {
        FlakySubscribeClusterCoordinator flaky = new FlakySubscribeClusterCoordinator();
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(CONFIG);
        QuorumBreakerRegistryBinder binder = new QuorumBreakerRegistryBinder(registry, flaky, Clock.systemUTC());
        binder.bindAll();
        awaitBound(registry, "heals-eventually");
        assertEquals(0, flaky.activeSubscriptions());

        triggerResubscribeCheck(binder);
        assertEquals(0, flaky.activeSubscriptions(), "still failing, a retry tick must not install anything yet");
        assertEquals(Set.of("heals-eventually"), binder.pendingResubscriptions());

        flaky.healSubscription();
        triggerResubscribeCheck(binder);

        assertEquals(1, flaky.activeSubscriptions(),
                "once the cluster is reachable, the retry must install the real subscription");
        assertTrue(binder.pendingResubscriptions().isEmpty(),
                "a breaker whose subscription just succeeded must no longer show up as pending");
    }

    @Test
    void removingABreaker_beforeItsSubscriptionEverSucceeded_stopsFurtherRetries() throws Exception {
        FlakySubscribeClusterCoordinator flaky = new FlakySubscribeClusterCoordinator();
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(CONFIG);
        QuorumBreakerRegistryBinder binder = new QuorumBreakerRegistryBinder(registry, flaky, Clock.systemUTC());
        binder.bindAll();
        awaitBound(registry, "removed-before-healing");

        registry.remove("removed-before-healing");
        flaky.healSubscription();
        triggerResubscribeCheck(binder);

        assertEquals(0, flaky.activeSubscriptions(),
                "a breaker removed before its subscription ever succeeded must not be resurrected by a later retry");
    }

    @Test
    void bind_afterClose_isANoOp() throws Exception {
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(CONFIG);
        QuorumBreakerRegistryBinder binder = new QuorumBreakerRegistryBinder(registry, cluster, Clock.systemUTC());
        binder.bindAll();
        binder.close();

        CircuitBreaker raw = registry.circuitBreaker("added-after-close");
        Method bind = QuorumBreakerRegistryBinder.class.getDeclaredMethod("bind", CircuitBreaker.class);
        bind.setAccessible(true);
        bind.invoke(binder, raw);

        assertEquals(0, binder.boundCount(),
                "a closed binder must not bind breakers even if the registry keeps notifying it");
        assertSame(raw, registry.circuitBreaker("added-after-close"),
                "a breaker seen after close() must stay a plain, unwrapped Resilience4j breaker");
    }

    @Test
    void constructor_doesNotDoubleWrap_anAlreadyWrappedSentryClusterCoordinator() throws Exception {
        ClusterCoordinator alreadyWrapped = new SentryClusterCoordinator(cluster);
        QuorumBreakerRegistryBinder binder = new QuorumBreakerRegistryBinder(
                CircuitBreakerRegistry.of(CONFIG), alreadyWrapped, Clock.systemUTC());

        Field clusterField = QuorumBreakerRegistryBinder.class.getDeclaredField("cluster");
        clusterField.setAccessible(true);
        assertSame(alreadyWrapped, clusterField.get(binder),
                "an already-wrapped SentryClusterCoordinator must not be wrapped a second time");
    }

    @Test
    void constructor_rejectsNegativeWindowTtlPadding() {
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(CONFIG);
        Clock clock = Clock.systemUTC();
        Duration negativePadding = Duration.ofSeconds(-1);

        assertThrows(IllegalArgumentException.class, () ->
                new QuorumBreakerRegistryBinder(registry, cluster, clock, negativePadding));
    }

    @Test
    void clusterCoordinatorGuard_exposesTheSentryCoordinatorsOwnGuardBreaker() {
        SentryClusterCoordinator alreadyWrapped = new SentryClusterCoordinator(cluster);
        QuorumBreakerRegistryBinder binder = new QuorumBreakerRegistryBinder(
                CircuitBreakerRegistry.of(CONFIG), alreadyWrapped, Clock.systemUTC());

        assertSame(alreadyWrapped.guard(), binder.clusterCoordinatorGuard());
    }

}
