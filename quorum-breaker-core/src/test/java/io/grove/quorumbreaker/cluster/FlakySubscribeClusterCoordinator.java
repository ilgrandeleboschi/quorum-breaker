package io.grove.quorumbreaker.cluster;

import io.grove.quorumbreaker.gate.ClaimParams;
import io.grove.quorumbreaker.gate.WindowOutcome;

import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public final class FlakySubscribeClusterCoordinator implements ClusterCoordinator {

    private final InMemoryClusterCoordinator delegate = new InMemoryClusterCoordinator();
    private final AtomicBoolean subscribeShouldSucceed = new AtomicBoolean(false);

    public void healSubscription() {
        subscribeShouldSucceed.set(true);
    }

    public int activeSubscriptions() {
        return delegate.activeSubscriptions();
    }

    @Override
    public CompletionStage<Void> publish(String channel, String message) {
        return delegate.publish(channel, message);
    }

    @Override
    public CompletionStage<AutoCloseable> subscribe(String channel, Consumer<String> listener) {
        if (!subscribeShouldSucceed.get()) {
            throw new RuntimeException("cluster unreachable for subscribe");
        }
        return delegate.subscribe(channel, listener);
    }

    @Override
    public CompletionStage<Boolean> claim(String key, ClaimParams params, Duration ttl) {
        return delegate.claim(key, params, ttl);
    }

    @Override
    public CompletionStage<WindowOutcome> trace(String key, ClaimParams params, boolean success, boolean slow, Duration ttl) {
        return delegate.trace(key, params, success, slow, ttl);
    }

}
