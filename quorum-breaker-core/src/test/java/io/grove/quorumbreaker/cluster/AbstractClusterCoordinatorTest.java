package io.grove.quorumbreaker.cluster;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AbstractClusterCoordinatorTest {

    @Test
    void defaultExecutor_runsTasksOnNamedVirtualThreads() throws Exception {
        AtomicReference<Thread> used = new AtomicReference<>();
        CompletableFuture.runAsync(() -> used.set(Thread.currentThread()), AbstractClusterCoordinator.defaultExecutor())
                .get(5, TimeUnit.SECONDS);

        assertTrue(used.get().isVirtual());
        assertTrue(used.get().getName().startsWith("quorum-breaker-cluster-"));
    }

    @Test
    void bounded_timesOut_wrapsInClusterCoordinatorException() {
        CompletionStage<String> neverCompletes = new CompletableFuture<>();

        ExecutionException ex = assertThrows(ExecutionException.class,
                () -> AbstractClusterCoordinator.bounded(Duration.ofMillis(20), neverCompletes)
                        .toCompletableFuture().get(5, TimeUnit.SECONDS));

        assertInstanceOf(ClusterCoordinatorException.class, ex.getCause());
        assertInstanceOf(TimeoutException.class, ex.getCause().getCause());
    }

}
