package io.grove.quorumbreaker.breaker;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.IllegalStateTransitionException;
import io.github.resilience4j.core.functions.CheckedConsumer;
import io.github.resilience4j.core.functions.CheckedRunnable;
import io.github.resilience4j.core.functions.CheckedSupplier;
import io.grove.quorumbreaker.gate.QuorumBreakerGate;
import io.grove.quorumbreaker.gate.WindowAdmission;
import io.grove.quorumbreaker.gate.WindowOutcome;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.github.resilience4j.circuitbreaker.CircuitBreaker.State.HALF_OPEN;
import static io.github.resilience4j.circuitbreaker.CircuitBreaker.State.OPEN;
import static io.grove.quorumbreaker.gate.WindowAdmission.COOLING_DOWN;
import static io.grove.quorumbreaker.gate.WindowAdmission.DENIED;
import static io.grove.quorumbreaker.gate.WindowAdmission.UNAVAILABLE;
import static io.grove.quorumbreaker.gate.WindowOutcome.CLOSE;
import static io.grove.quorumbreaker.gate.WindowOutcome.REOPEN;

@Slf4j
public final class QuorumBreaker implements CircuitBreaker {

    @Delegate
    private final CircuitBreaker delegate;
    private final QuorumBreakerGate gate;
    private final long slowThresholdInTimestampUnit;

    public QuorumBreaker(CircuitBreaker delegate, QuorumBreakerGate gate) {
        this.delegate = delegate;
        this.gate = gate;
        CircuitBreakerConfig cfg = delegate.getCircuitBreakerConfig();
        this.slowThresholdInTimestampUnit = delegate.getTimestampUnit().convert(cfg.getSlowCallDurationThreshold());
    }

    @Override
    public <T> T executeCheckedSupplier(CheckedSupplier<T> checkedSupplier) throws Throwable {
        if (isNotGated(delegate.getState())) {
            return delegate.executeCheckedSupplier(checkedSupplier);
        }
        WindowAdmission admission = gate.requireAdmission().toCompletableFuture().join();
        if (admission == COOLING_DOWN || admission == DENIED) {
            throw denied(admission);
        }
        if (admission == UNAVAILABLE) {
            return delegate.executeCheckedSupplier(checkedSupplier);
        }
        return runInWindow(checkedSupplier);
    }

    private <T> T runInWindow(CheckedSupplier<T> checkedSupplier) throws Throwable {
        ensureHalfOpen();
        long start = delegate.getCurrentTimestamp();
        try {
            T result = checkedSupplier.get();
            long elapsed = delegate.getCurrentTimestamp() - start;
            delegate.onSuccess(elapsed, delegate.getTimestampUnit());
            applyDecision(gate.reportOutcome(true, isSlow(elapsed)).toCompletableFuture().join());
            return result;
        } catch (Throwable t) {
            long elapsed = delegate.getCurrentTimestamp() - start;
            delegate.onError(elapsed, delegate.getTimestampUnit(), t);
            applyDecision(gate.reportOutcome(false, isSlow(elapsed)).toCompletableFuture().join());
            throw t;
        }
    }

    @Override
    public <T> CompletionStage<T> executeCompletionStage(Supplier<CompletionStage<T>> supplier) {
        if (isNotGated(delegate.getState())) {
            return delegate.executeCompletionStage(supplier);
        }
        return gate.requireAdmission().thenCompose(admission -> {
            if (admission == COOLING_DOWN || admission == DENIED) {
                CompletableFuture<T> failed = new CompletableFuture<>();
                failed.completeExceptionally(denied(admission));
                return failed;
            }
            if (admission == UNAVAILABLE) {
                return delegate.executeCompletionStage(supplier);
            }
            return runInWindowAsync(supplier);
        });
    }

    private <T> CompletionStage<T> runInWindowAsync(Supplier<CompletionStage<T>> supplier) {
        ensureHalfOpen();
        long start = delegate.getCurrentTimestamp();
        return supplier.get().whenComplete((result, throwable) -> {
            long elapsed = delegate.getCurrentTimestamp() - start;
            boolean success = throwable == null;
            if (success) {
                delegate.onSuccess(elapsed, delegate.getTimestampUnit());
            } else {
                delegate.onError(elapsed, delegate.getTimestampUnit(), unwrap(throwable));
            }
            gate.reportOutcome(success, isSlow(elapsed)).thenAccept(this::applyDecision);
        });
    }

    private boolean isSlow(long elapsed) {
        if (delegate.getCircuitBreakerConfig().getSlowCallRateThreshold() >= 100f) return false;
        return elapsed >= slowThresholdInTimestampUnit;
    }

    @Override
    public <T> CheckedSupplier<T> decorateCheckedSupplier(CheckedSupplier<T> checkedSupplier) {
        return () -> executeCheckedSupplier(checkedSupplier);
    }

    @Override
    public <T> T executeSupplier(Supplier<T> supplier) {
        return unchecked(supplier::get);
    }

    @Override
    public <T> Supplier<T> decorateSupplier(Supplier<T> supplier) {
        return () -> unchecked(supplier::get);
    }

    @Override
    public <T> T executeCallable(Callable<T> callable) throws Exception {
        return unchecked(callable::call);
    }

    @Override
    public <T> Callable<T> decorateCallable(Callable<T> callable) {
        return () -> unchecked(callable::call);
    }

    @Override
    public void executeRunnable(Runnable runnable) {
        unchecked(asCheckedSupplier(runnable));
    }

    @Override
    public Runnable decorateRunnable(Runnable runnable) {
        return () -> unchecked(asCheckedSupplier(runnable));
    }

    @Override
    public void executeCheckedRunnable(CheckedRunnable runnable) throws Throwable {
        executeCheckedSupplier(asCheckedSupplier(runnable));
    }

    @Override
    public CheckedRunnable decorateCheckedRunnable(CheckedRunnable runnable) {
        return () -> executeCheckedSupplier(asCheckedSupplier(runnable));
    }

    @Override
    public <T> Consumer<T> decorateConsumer(Consumer<T> consumer) {
        return t -> unchecked(asCheckedSupplier(consumer, t));
    }

    @Override
    public <T> CheckedConsumer<T> decorateCheckedConsumer(CheckedConsumer<T> consumer) {
        return t -> executeCheckedSupplier(asCheckedSupplier(consumer, t));
    }

    @Override
    public <T> Supplier<CompletionStage<T>> decorateCompletionStage(Supplier<CompletionStage<T>> supplier) {
        return () -> executeCompletionStage(supplier);
    }

    @Override
    public <T> Supplier<Future<T>> decorateFuture(Supplier<Future<T>> supplier) {
        return () -> executeFuture(supplier);
    }

    private <T> Future<T> executeFuture(Supplier<Future<T>> supplier) {
        if (isNotGated(delegate.getState())) {
            return delegate.decorateFuture(supplier).get();
        }
        WindowAdmission admission = gate.requireAdmission().toCompletableFuture().join();
        if (admission == COOLING_DOWN || admission == DENIED) {
            CompletableFuture<T> failed = new CompletableFuture<>();
            failed.completeExceptionally(denied(admission));
            return failed;
        }
        if (admission == UNAVAILABLE) {
            return delegate.decorateFuture(supplier).get();
        }
        return runInWindowFuture(supplier);
    }

    private <T> Future<T> runInWindowFuture(Supplier<Future<T>> supplier) {
        ensureHalfOpen();
        long start = delegate.getCurrentTimestamp();
        try {
            return new GatedFuture<>(supplier.get(), start);
        } catch (Throwable t) {
            long elapsed = delegate.getCurrentTimestamp() - start;
            delegate.onError(elapsed, delegate.getTimestampUnit(), t);
            applyDecision(gate.reportOutcome(false, isSlow(elapsed)).toCompletableFuture().join());
            throw t;
        }
    }

    private final class GatedFuture<T> implements Future<T> {

        private final Future<T> future;
        private final long start;
        private final AtomicBoolean reported = new AtomicBoolean(false);

        GatedFuture(Future<T> future, long start) {
            this.future = future;
            this.start = start;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return future.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return future.isCancelled();
        }

        @Override
        public boolean isDone() {
            return future.isDone();
        }

        @Override
        public T get() throws InterruptedException, ExecutionException {
            try {
                T result = future.get();
                reportOnce(true, null);
                return result;
            } catch (ExecutionException e) {
                reportOnce(false, unwrap(e));
                throw e;
            } catch (CancellationException e) {
                reportOnce(false, e);
                throw e;
            }
        }

        @Override
        public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            try {
                T result = future.get(timeout, unit);
                reportOnce(true, null);
                return result;
            } catch (TimeoutException | CancellationException e) {
                reportOnce(false, e);
                throw e;
            } catch (ExecutionException e) {
                reportOnce(false, unwrap(e));
                throw e;
            }
        }

        private void reportOnce(boolean success, Throwable cause) {
            if (!reported.compareAndSet(false, true)) return;
            long elapsed = delegate.getCurrentTimestamp() - start;
            if (success) {
                delegate.onSuccess(elapsed, delegate.getTimestampUnit());
            } else {
                delegate.onError(elapsed, delegate.getTimestampUnit(), cause);
            }
            gate.reportOutcome(success, isSlow(elapsed)).thenAccept(QuorumBreaker.this::applyDecision);
        }
    }

    private <T> T unchecked(CheckedSupplier<T> call) {
        try {
            return executeCheckedSupplier(call);
        } catch (Throwable t) {
            return CheckedSupplier.sneakyThrow(t);
        }
    }

    private static CheckedSupplier<Void> asCheckedSupplier(Runnable runnable) {
        return () -> {
            runnable.run();
            return null;
        };
    }

    private static CheckedSupplier<Void> asCheckedSupplier(CheckedRunnable runnable) {
        return () -> {
            runnable.run();
            return null;
        };
    }

    private static <T> CheckedSupplier<Void> asCheckedSupplier(Consumer<T> consumer, T input) {
        return () -> {
            consumer.accept(input);
            return null;
        };
    }

    private static <T> CheckedSupplier<Void> asCheckedSupplier(CheckedConsumer<T> consumer, T input) {
        return () -> {
            consumer.accept(input);
            return null;
        };
    }

    private void applyDecision(WindowOutcome decision) {
        if (decision == CLOSE) {
            delegate.transitionToClosedState();
            gate.announceClose();
        } else if (decision == REOPEN) {
            delegate.transitionToOpenState();
            gate.announceOpen();
        }
    }

    private CallNotPermittedException denied(WindowAdmission admission) {
        if (admission == DENIED) {
            ensureHalfOpen();
        }
        return CallNotPermittedException.createCallNotPermittedException(delegate);
    }

    private void ensureHalfOpen() {
        if (delegate.getState() == HALF_OPEN) return;
        try {
            delegate.transitionToHalfOpenState();
        } catch (IllegalStateTransitionException alreadyResolved) {
            log.debug("QuorumBreaker[{}]: lost a race to move to HALF_OPEN - a concurrent cluster " +
                    "decision already resolved this breaker to {}", delegate.getName(), delegate.getState());
        }
    }

    private static Throwable unwrap(Throwable t) {
        boolean unwrappable = t instanceof CompletionException || t instanceof ExecutionException;
        return unwrappable && t.getCause() != null ? t.getCause() : t;
    }

    private static boolean isNotGated(State state) {
        return state != OPEN && state != HALF_OPEN;
    }

}
