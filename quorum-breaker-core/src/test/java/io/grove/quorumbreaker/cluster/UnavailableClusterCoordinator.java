package io.grove.quorumbreaker.cluster;

import io.grove.quorumbreaker.gate.ClaimParams;
import io.grove.quorumbreaker.gate.WindowOutcome;

import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

public final class UnavailableClusterCoordinator implements ClusterCoordinator {

    @Override
    public CompletionStage<Void> publish(String channel, String message) {
        throw new RuntimeException("cluster unreachable");
    }

    @Override
    public CompletionStage<AutoCloseable> subscribe(String channel, Consumer<String> listener) {
        throw new RuntimeException("cluster unreachable");
    }

    @Override
    public CompletionStage<Boolean> claim(String key, ClaimParams params, Duration ttl) {
        throw new RuntimeException("cluster unreachable");
    }

    @Override
    public CompletionStage<WindowOutcome> trace(String key, ClaimParams params, boolean success, boolean slow, Duration ttl) {
        throw new RuntimeException("cluster unreachable");
    }

}
