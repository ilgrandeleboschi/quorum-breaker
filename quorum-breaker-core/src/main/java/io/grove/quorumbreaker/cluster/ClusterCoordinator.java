package io.grove.quorumbreaker.cluster;

import io.grove.quorumbreaker.gate.ClaimParams;
import io.grove.quorumbreaker.gate.WindowOutcome;

import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

public interface ClusterCoordinator {

    CompletionStage<Void> publish(String channel, String message);

    CompletionStage<AutoCloseable> subscribe(String channel, Consumer<String> listener);

    CompletionStage<Boolean> claim(String key, ClaimParams params, Duration ttl);

    CompletionStage<WindowOutcome> trace(String key, ClaimParams params, boolean success, boolean slow, Duration ttl);

}
