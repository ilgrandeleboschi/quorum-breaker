package io.grove.quorumbreaker.breaker;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.grove.quorumbreaker.gate.QuorumBreakerGate;
import io.grove.quorumbreaker.gate.WindowAdmission;
import io.grove.quorumbreaker.gate.WindowOutcome;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class QuorumBreakerTest {

    @Mock
    private QuorumBreakerGate gate;

    private static CircuitBreaker newBreaker(String name) {
        CircuitBreakerConfig cfg = CircuitBreakerConfig.custom()
                .slidingWindowSize(2)
                .minimumNumberOfCalls(2)
                .failureRateThreshold(50f)
                .waitDurationInOpenState(Duration.ofMinutes(5))
                .permittedNumberOfCallsInHalfOpenState(2)
                .build();
        return CircuitBreakerRegistry.of(cfg).circuitBreaker(name);
    }

    @Test
    void executeCheckedSupplier_closed_neverTouchesTheClusterGate() throws Throwable {
        CircuitBreaker delegate = newBreaker("closed-sync");
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        assertEquals("ok", breaker.executeCheckedSupplier(() -> "ok"));
        verify(gate, never()).requireAdmission();
    }

    @Test
    void executeCheckedSupplier_disabled_neverTouchesTheClusterGate() throws Throwable {
        CircuitBreaker delegate = newBreaker("disabled-sync");
        delegate.transitionToDisabledState();
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        assertEquals("ok", breaker.executeCheckedSupplier(() -> "ok"));
        verify(gate, never()).requireAdmission();
    }

    @Test
    void executeCheckedSupplier_metricsOnly_neverTouchesTheClusterGate() throws Throwable {
        CircuitBreaker delegate = newBreaker("metrics-only-sync");
        delegate.transitionToMetricsOnlyState();
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        assertEquals("ok", breaker.executeCheckedSupplier(() -> "ok"));
        verify(gate, never()).requireAdmission();
    }

    @Test
    void executeCheckedSupplier_forcedOpen_rejectsWithoutTouchingTheClusterGate() {
        CircuitBreaker delegate = newBreaker("forced-open-sync");
        delegate.transitionToForcedOpenState();
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        assertThrows(CallNotPermittedException.class, () -> breaker.executeCheckedSupplier(() -> "ok"));
        verify(gate, never()).requireAdmission();
    }

    @Test
    void executeCompletionStage_closed_neverTouchesTheClusterGate() {
        CircuitBreaker delegate = newBreaker("closed-async");
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        CompletionStage<Object> result = breaker.executeCompletionStage(() -> CompletableFuture.completedFuture("ok"));

        assertEquals("ok", result.toCompletableFuture().join());
        verify(gate, never()).requireAdmission();
    }

    @Test
    void executeCompletionStage_disabled_neverTouchesTheClusterGate() {
        CircuitBreaker delegate = newBreaker("disabled-async");
        delegate.transitionToDisabledState();
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        CompletionStage<Object> result = breaker.executeCompletionStage(() -> CompletableFuture.completedFuture("ok"));

        assertEquals("ok", result.toCompletableFuture().join());
        verify(gate, never()).requireAdmission();
    }

    @Test
    void executeCompletionStage_metricsOnly_neverTouchesTheClusterGate() {
        CircuitBreaker delegate = newBreaker("metrics-only-async");
        delegate.transitionToMetricsOnlyState();
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        CompletionStage<Object> result = breaker.executeCompletionStage(() -> CompletableFuture.completedFuture("ok"));

        assertEquals("ok", result.toCompletableFuture().join());
        verify(gate, never()).requireAdmission();
    }

    @Test
    void executeCompletionStage_forcedOpen_rejectsWithoutTouchingTheClusterGate() {
        CircuitBreaker delegate = newBreaker("forced-open-async");
        delegate.transitionToForcedOpenState();
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        CompletionStage<Object> result = breaker.executeCompletionStage(() -> CompletableFuture.completedFuture("ok"));

        ExecutionException ex = assertThrows(ExecutionException.class, () -> result.toCompletableFuture().get());
        assertInstanceOf(CallNotPermittedException.class, ex.getCause());
        verify(gate, never()).requireAdmission();
    }

    private static void forceOpen(CircuitBreaker cb) {
        cb.transitionToOpenState();
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());
    }

    @Test
    void executeCheckedSupplier_granted_successfulCall_appliesCloseDecision() throws Throwable {
        CircuitBreaker delegate = newBreaker("granted-success");
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.GRANTED));
        when(gate.reportOutcome(true, false)).thenReturn(CompletableFuture.completedFuture(WindowOutcome.CLOSE));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        assertEquals("ok", breaker.executeCheckedSupplier(() -> "ok"));

        assertEquals(CircuitBreaker.State.CLOSED, delegate.getState());
        verify(gate).reportOutcome(true, false);
    }

    @Test
    void executeCheckedSupplier_granted_failedCall_appliesReopenDecision() {
        CircuitBreaker delegate = newBreaker("granted-fail");
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.GRANTED));
        when(gate.reportOutcome(false, false)).thenReturn(CompletableFuture.completedFuture(WindowOutcome.REOPEN));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        assertThrows(IllegalStateException.class, () -> breaker.executeCheckedSupplier(() -> {
            throw new IllegalStateException("boom");
        }));

        assertEquals(CircuitBreaker.State.OPEN, delegate.getState());
    }

    @Test
    void executeCheckedSupplier_granted_pendingDecision_staysHalfOpen() throws Throwable {
        CircuitBreaker delegate = newBreaker("granted-pending");
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.GRANTED));
        when(gate.reportOutcome(true, false)).thenReturn(CompletableFuture.completedFuture(WindowOutcome.PENDING));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        assertEquals("ok", breaker.executeCheckedSupplier(() -> "ok"));

        assertEquals(CircuitBreaker.State.HALF_OPEN, delegate.getState());
    }

    @Test
    void executeCheckedSupplier_denied_throwsCallNotPermitted_withoutCallingDelegate() {
        CircuitBreaker delegate = newBreaker("denied-sync");
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.DENIED));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        assertThrows(CallNotPermittedException.class, () -> breaker.executeCheckedSupplier(() -> {
            throw new AssertionError("must not execute the guarded call when denied");
        }));
        verify(gate, never()).reportOutcome(anyBoolean(), anyBoolean());
        assertEquals(CircuitBreaker.State.HALF_OPEN, delegate.getState(),
                "the cluster budget was exhausted, meaning recovery testing is genuinely underway cluster-wide");
    }

    @Test
    void executeCheckedSupplier_coolingDown_throwsCallNotPermitted_withoutTransitioningState() {
        CircuitBreaker delegate = newBreaker("cooling-down-sync");
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.COOLING_DOWN));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        assertThrows(CallNotPermittedException.class, () -> breaker.executeCheckedSupplier(() -> {
            throw new AssertionError("must not execute the guarded call while still cooling down");
        }));
        verify(gate, never()).reportOutcome(anyBoolean(), anyBoolean());
        assertEquals(CircuitBreaker.State.OPEN, delegate.getState(),
                "recovery testing hasn't started anywhere in the cluster yet - must not claim otherwise");
    }

    @Test
    void executeCheckedSupplier_unavailable_fallsBackToLocalCircuitBreaker() {
        CircuitBreaker delegate = newBreaker("unavailable-sync");
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.UNAVAILABLE));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        assertThrows(CallNotPermittedException.class, () -> breaker.executeCheckedSupplier(() -> "ok"));
        verify(gate, never()).reportOutcome(anyBoolean(), anyBoolean());
    }

    @Test
    void executeCompletionStage_granted_successfulCall_appliesCloseDecision() {
        CircuitBreaker delegate = newBreaker("granted-success-async");
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.GRANTED));
        when(gate.reportOutcome(true, false)).thenReturn(CompletableFuture.completedFuture(WindowOutcome.CLOSE));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        CompletionStage<Object> result = breaker.executeCompletionStage(() -> CompletableFuture.completedFuture("ok"));

        assertEquals("ok", result.toCompletableFuture().join());
        assertEquals(CircuitBreaker.State.CLOSED, delegate.getState());
    }

    @Test
    void executeCompletionStage_granted_failedCall_appliesReopenDecision() {
        CircuitBreaker delegate = newBreaker("granted-fail-async");
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.GRANTED));
        when(gate.reportOutcome(false, false)).thenReturn(CompletableFuture.completedFuture(WindowOutcome.REOPEN));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        CompletableFuture<Object> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IllegalStateException("boom"));
        CompletionStage<Object> result = breaker.executeCompletionStage(() -> failed);

        ExecutionException ex = assertThrows(ExecutionException.class, () -> result.toCompletableFuture().get());
        assertInstanceOf(IllegalStateException.class, ex.getCause());
        assertEquals(CircuitBreaker.State.OPEN, delegate.getState());
    }

    @Test
    void executeCompletionStage_wrappedCompletionException_unwrapsBeforeRecordingOnDelegate() {
        CircuitBreaker delegate = spy(newBreaker("unwrap-async"));
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.GRANTED));
        when(gate.reportOutcome(false, false)).thenReturn(CompletableFuture.completedFuture(WindowOutcome.PENDING));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        IllegalStateException cause = new IllegalStateException("boom");
        CompletableFuture<Object> failed = new CompletableFuture<>();
        failed.completeExceptionally(new CompletionException(cause));
        CompletionStage<Object> result = breaker.executeCompletionStage(() -> failed);

        assertThrows(ExecutionException.class, () -> result.toCompletableFuture().get());
        verify(delegate).onError(anyLong(), any(TimeUnit.class), same(cause));
    }

    @Test
    void executeCompletionStage_denied_completesExceptionallyWithCallNotPermitted() {
        CircuitBreaker delegate = newBreaker("denied-async");
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.DENIED));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        Supplier<CompletionStage<Object>> supplier = () -> {
            throw new AssertionError("must not execute the guarded call when denied");
        };
        CompletionStage<Object> result = breaker.executeCompletionStage(supplier);

        ExecutionException ex = assertThrows(ExecutionException.class, () -> result.toCompletableFuture().get());
        assertInstanceOf(CallNotPermittedException.class, ex.getCause());
    }

    @Test
    void executeCompletionStage_unavailable_fallsBackToLocalCircuitBreaker() {
        CircuitBreaker delegate = newBreaker("unavailable-async");
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.UNAVAILABLE));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        CompletionStage<Object> result = breaker.executeCompletionStage(() -> CompletableFuture.completedFuture("ok"));

        ExecutionException ex = assertThrows(ExecutionException.class, () -> result.toCompletableFuture().get());
        assertInstanceOf(CallNotPermittedException.class, ex.getCause());
    }

    @Test
    void executeCheckedSupplier_grantedWhileAlreadyHalfOpen_doesNotRetransition() throws Throwable {
        CircuitBreaker delegate = newBreaker("already-half-open");
        forceOpen(delegate);
        delegate.transitionToHalfOpenState();
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.GRANTED));
        when(gate.reportOutcome(true, false)).thenReturn(CompletableFuture.completedFuture(WindowOutcome.PENDING));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        assertEquals("ok", breaker.executeCheckedSupplier(() -> "ok"));
        assertEquals(CircuitBreaker.State.HALF_OPEN, delegate.getState());
    }

    @Test
    void executeCheckedSupplier_singleNodeClaimsTheEntireHalfOpenBudget_localAutoTransitionAgreesWithGateDecision()
            throws Throwable {
        // Resilience4j's own delegate keeps its own permittedNumberOfCallsInHalfOpenState-sized
        // half-open metrics window and can transition on its own once it has recorded that many
        // local onSuccess/onError calls - regardless of what the (mocked) gate below decides. That
        // can only happen if this single node handled every permitted call itself, i.e. the local
        // and cluster-wide outcome sets are identical, so the two are expected to always agree.
        CircuitBreaker delegate = newBreaker("single-node-claims-all"); // permittedNumberOfCallsInHalfOpenState(2)
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.GRANTED));
        when(gate.reportOutcome(true, false))
                .thenReturn(CompletableFuture.completedFuture(WindowOutcome.PENDING))
                .thenReturn(CompletableFuture.completedFuture(WindowOutcome.CLOSE));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        breaker.executeCheckedSupplier(() -> "ok");
        assertEquals(CircuitBreaker.State.HALF_OPEN, delegate.getState(),
                "must not close before the shared window has enough outcomes to decide");

        breaker.executeCheckedSupplier(() -> "ok");
        assertEquals(CircuitBreaker.State.CLOSED, delegate.getState());
    }

    @Test
    void delegatedMethods_passThroughToTheWrappedBreaker() {
        CircuitBreaker delegate = newBreaker("delegate-passthrough");
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        assertEquals(delegate.getName(), breaker.getName());
        assertEquals(delegate.getState(), breaker.getState());
        assertEquals(delegate.getCircuitBreakerConfig(), breaker.getCircuitBreakerConfig());
    }

    @Test
    void executeSupplier_granted_successfulCall_appliesCloseDecision() {
        CircuitBreaker delegate = newBreaker("supplier-granted");
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.GRANTED));
        when(gate.reportOutcome(true, false)).thenReturn(CompletableFuture.completedFuture(WindowOutcome.CLOSE));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        assertEquals("ok", breaker.executeSupplier(() -> "ok"));

        assertEquals(CircuitBreaker.State.CLOSED, delegate.getState());
        verify(gate).reportOutcome(true, false);
    }

    @Test
    void decorateSupplier_denied_throwsCallNotPermitted_withoutCallingTheSupplier() {
        CircuitBreaker delegate = newBreaker("supplier-denied");
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.DENIED));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        Supplier<String> decorated = breaker.decorateSupplier(() -> {
            throw new AssertionError("must not execute the guarded call when denied");
        });

        assertThrows(CallNotPermittedException.class, decorated::get);
    }

    @Test
    void executeCallable_granted_failedCall_appliesReopenDecision() {
        CircuitBreaker delegate = newBreaker("callable-granted-fail");
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.GRANTED));
        when(gate.reportOutcome(false, false)).thenReturn(CompletableFuture.completedFuture(WindowOutcome.REOPEN));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        assertThrows(IllegalStateException.class, () -> breaker.executeCallable(() -> {
            throw new IllegalStateException("boom");
        }));

        assertEquals(CircuitBreaker.State.OPEN, delegate.getState());
    }

    @Test
    void decorateCallable_granted_successfulCall_appliesCloseDecision() throws Exception {
        CircuitBreaker delegate = newBreaker("callable-granted");
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.GRANTED));
        when(gate.reportOutcome(true, false)).thenReturn(CompletableFuture.completedFuture(WindowOutcome.CLOSE));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        assertEquals("ok", breaker.decorateCallable(() -> "ok").call());

        assertEquals(CircuitBreaker.State.CLOSED, delegate.getState());
    }

    @Test
    void executeRunnable_granted_successfulCall_appliesCloseDecision() {
        CircuitBreaker delegate = newBreaker("runnable-granted");
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.GRANTED));
        when(gate.reportOutcome(true, false)).thenReturn(CompletableFuture.completedFuture(WindowOutcome.CLOSE));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        breaker.executeRunnable(() -> {
        });

        assertEquals(CircuitBreaker.State.CLOSED, delegate.getState());
        verify(gate).reportOutcome(true, false);
    }

    @Test
    void decorateRunnable_denied_throwsCallNotPermitted_withoutRunning() {
        CircuitBreaker delegate = newBreaker("runnable-denied");
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.DENIED));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        Runnable decorated = breaker.decorateRunnable(() -> {
            throw new AssertionError("must not run when denied");
        });

        assertThrows(CallNotPermittedException.class, decorated::run);
    }

    @Test
    void executeCheckedRunnable_granted_successfulCall_appliesCloseDecision() throws Throwable {
        CircuitBreaker delegate = newBreaker("checked-runnable-granted");
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.GRANTED));
        when(gate.reportOutcome(true, false)).thenReturn(CompletableFuture.completedFuture(WindowOutcome.CLOSE));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        breaker.executeCheckedRunnable(() -> {
        });

        assertEquals(CircuitBreaker.State.CLOSED, delegate.getState());
        verify(gate).reportOutcome(true, false);
    }

    @Test
    void decorateCheckedRunnable_granted_failedCall_appliesReopenDecision() {
        CircuitBreaker delegate = newBreaker("checked-runnable-granted-fail");
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.GRANTED));
        when(gate.reportOutcome(false, false)).thenReturn(CompletableFuture.completedFuture(WindowOutcome.REOPEN));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        var decorated = breaker.decorateCheckedRunnable(() -> {
            throw new IllegalStateException("boom");
        });

        assertThrows(IllegalStateException.class, decorated::run);
        assertEquals(CircuitBreaker.State.OPEN, delegate.getState());
    }

    @Test
    void decorateCheckedSupplier_granted_successfulCall_appliesCloseDecision() throws Throwable {
        CircuitBreaker delegate = newBreaker("checked-supplier-granted");
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.GRANTED));
        when(gate.reportOutcome(true, false)).thenReturn(CompletableFuture.completedFuture(WindowOutcome.CLOSE));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        assertEquals("ok", breaker.decorateCheckedSupplier(() -> "ok").get());

        assertEquals(CircuitBreaker.State.CLOSED, delegate.getState());
    }

    @Test
    void decorateConsumer_granted_successfulCall_appliesCloseDecision() {
        CircuitBreaker delegate = newBreaker("consumer-granted");
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.GRANTED));
        when(gate.reportOutcome(true, false)).thenReturn(CompletableFuture.completedFuture(WindowOutcome.CLOSE));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        breaker.decorateConsumer((String s) -> assertEquals("input", s)).accept("input");

        assertEquals(CircuitBreaker.State.CLOSED, delegate.getState());
        verify(gate).reportOutcome(true, false);
    }

    @Test
    void decorateCheckedConsumer_denied_throwsCallNotPermitted_withoutAccepting() {
        CircuitBreaker delegate = newBreaker("checked-consumer-denied");
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.DENIED));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        var decorated = breaker.decorateCheckedConsumer((String s) -> {
            throw new AssertionError("must not accept when denied");
        });

        assertThrows(CallNotPermittedException.class, () -> decorated.accept("input"));
    }

    @Test
    void decorateCompletionStage_granted_successfulCall_appliesCloseDecision() {
        CircuitBreaker delegate = newBreaker("completion-stage-decorate-granted");
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.GRANTED));
        when(gate.reportOutcome(true, false)).thenReturn(CompletableFuture.completedFuture(WindowOutcome.CLOSE));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        Supplier<CompletionStage<String>> decorated =
                breaker.decorateCompletionStage(() -> CompletableFuture.completedFuture("ok"));

        assertEquals("ok", decorated.get().toCompletableFuture().join());
        assertEquals(CircuitBreaker.State.CLOSED, delegate.getState());
    }

    @Test
    void decorateFuture_denied_throwsCallNotPermitted_withoutTouchingSupplier() {
        CircuitBreaker delegate = newBreaker("future-denied");
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.DENIED));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        Supplier<Future<String>> decorated = breaker.decorateFuture(() -> {
            throw new AssertionError("must not execute the guarded call when denied");
        });

        Future<String> future = decorated.get();
        ExecutionException ex = assertThrows(ExecutionException.class, future::get);
        assertInstanceOf(CallNotPermittedException.class, ex.getCause());
    }

    @Test
    void decorateFuture_unavailable_fallsBackToLocalCircuitBreaker() {
        CircuitBreaker delegate = newBreaker("future-unavailable");
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.UNAVAILABLE));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        Supplier<Future<String>> decorated = breaker.decorateFuture(() -> CompletableFuture.completedFuture("ok"));
        Future<String> future = decorated.get();

        ExecutionException ex = assertThrows(ExecutionException.class, future::get);
        assertInstanceOf(CallNotPermittedException.class, ex.getCause());
    }

    @Test
    void decorateFuture_granted_successfulCall_appliesCloseDecision() throws Exception {
        CircuitBreaker delegate = newBreaker("future-granted-success");
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.GRANTED));
        when(gate.reportOutcome(true, false)).thenReturn(CompletableFuture.completedFuture(WindowOutcome.CLOSE));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        Supplier<Future<String>> decorated =
                breaker.decorateFuture(() -> CompletableFuture.completedFuture("ok"));
        Future<String> future = decorated.get();

        assertEquals("ok", future.get());
        assertEquals(CircuitBreaker.State.CLOSED, delegate.getState());
        verify(gate).reportOutcome(true, false);
    }

    @Test
    void decorateFuture_granted_failedCall_appliesReopenDecision() {
        CircuitBreaker delegate = newBreaker("future-granted-fail");
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.GRANTED));
        when(gate.reportOutcome(false, false)).thenReturn(CompletableFuture.completedFuture(WindowOutcome.REOPEN));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        CompletableFuture<String> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IllegalStateException("boom"));
        Supplier<Future<String>> decorated = breaker.decorateFuture(() -> failed);
        Future<String> future = decorated.get();

        ExecutionException ex = assertThrows(ExecutionException.class, future::get);
        assertInstanceOf(IllegalStateException.class, ex.getCause());
        assertEquals(CircuitBreaker.State.OPEN, delegate.getState());
    }

    @Test
    void decorateFuture_get_calledTwice_reportsOutcomeOnlyOnce() throws Exception {
        CircuitBreaker delegate = newBreaker("future-report-once");
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.GRANTED));
        when(gate.reportOutcome(true, false)).thenReturn(CompletableFuture.completedFuture(WindowOutcome.CLOSE));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        Supplier<Future<String>> decorated =
                breaker.decorateFuture(() -> CompletableFuture.completedFuture("ok"));
        Future<String> future = decorated.get();

        future.get();
        future.get();

        verify(gate, times(1)).reportOutcome(true, false);
    }

    @Test
    void decorateFuture_cancelledThenGet_reportsOutcomeAsFailure() {
        CircuitBreaker delegate = newBreaker("future-cancelled");
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.GRANTED));
        when(gate.reportOutcome(false, false)).thenReturn(CompletableFuture.completedFuture(WindowOutcome.PENDING));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        CompletableFuture<String> neverCompletes = new CompletableFuture<>();
        Supplier<Future<String>> decorated = breaker.decorateFuture(() -> neverCompletes);
        Future<String> future = decorated.get();

        future.cancel(true);

        assertThrows(CancellationException.class, future::get);
        verify(gate).reportOutcome(false, false);
    }

    @Test
    void decorateFuture_cancelledAndNeverGet_doesNotReportOutcome() {
        CircuitBreaker delegate = newBreaker("future-cancelled-no-get");
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.GRANTED));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        CompletableFuture<String> neverCompletes = new CompletableFuture<>();
        Supplier<Future<String>> decorated = breaker.decorateFuture(() -> neverCompletes);
        Future<String> future = decorated.get();

        future.cancel(true);

        verify(gate, never()).reportOutcome(anyBoolean(), anyBoolean());
    }

    private static CircuitBreaker newBreaker(String name, float slowCallRateThreshold,
                                              Duration slowCallDurationThreshold, AtomicLong timestampMillis) {
        CircuitBreakerConfig cfg = CircuitBreakerConfig.custom()
                .slidingWindowSize(2)
                .minimumNumberOfCalls(2)
                .failureRateThreshold(50f)
                .slowCallRateThreshold(slowCallRateThreshold)
                .slowCallDurationThreshold(slowCallDurationThreshold)
                .waitDurationInOpenState(Duration.ofMinutes(5))
                .permittedNumberOfCallsInHalfOpenState(2)
                .currentTimestampFunction(ignoredClock -> timestampMillis.get(), TimeUnit.MILLISECONDS)
                .build();
        return CircuitBreakerRegistry.of(cfg).circuitBreaker(name);
    }

    @Test
    void executeCheckedSupplier_granted_slowSuccessfulCall_reportsSlowToTheGate() throws Throwable {
        AtomicLong timestampMillis = new AtomicLong(0);
        CircuitBreaker delegate = newBreaker("slow-enabled", 50f, Duration.ofMillis(5), timestampMillis);
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.GRANTED));
        when(gate.reportOutcome(true, true)).thenReturn(CompletableFuture.completedFuture(WindowOutcome.CLOSE));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        assertEquals("ok", breaker.executeCheckedSupplier(() -> {
            timestampMillis.addAndGet(50);
            return "ok";
        }));

        verify(gate).reportOutcome(true, true);
    }

    @Test
    void executeCheckedSupplier_granted_slowCallDetectionDisabled_neverReportsSlow_evenIfTheCallIsSlow() throws Throwable {
        AtomicLong timestampMillis = new AtomicLong(0);
        CircuitBreaker delegate = newBreaker("slow-disabled", 100f, Duration.ofMillis(5), timestampMillis);
        forceOpen(delegate);
        when(gate.requireAdmission()).thenReturn(CompletableFuture.completedFuture(WindowAdmission.GRANTED));
        when(gate.reportOutcome(true, false)).thenReturn(CompletableFuture.completedFuture(WindowOutcome.CLOSE));
        QuorumBreaker breaker = new QuorumBreaker(delegate, gate);

        assertEquals("ok", breaker.executeCheckedSupplier(() -> {
            timestampMillis.addAndGet(50);
            return "ok";
        }));

        verify(gate).reportOutcome(true, false);
    }

}
