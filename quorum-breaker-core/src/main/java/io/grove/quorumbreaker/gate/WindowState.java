package io.grove.quorumbreaker.gate;

import java.util.Optional;

import static io.grove.quorumbreaker.gate.WindowOutcome.CLOSE;
import static io.grove.quorumbreaker.gate.WindowOutcome.PENDING;
import static io.grove.quorumbreaker.gate.WindowOutcome.REOPEN;

public record WindowState(String generation, int claimed, Counters counters) {

    public static WindowState fresh(String generation) {
        return new WindowState(generation, 0, new Counters(0, 0, 0));
    }

    public WindowState claim() {
        return new WindowState(generation, claimed + 1, counters);
    }

    public WindowState release() {
        return new WindowState(generation, claimed - 1, counters);
    }

    public WindowState recordOutcome(boolean success, boolean slow) {
        return new WindowState(generation, claimed, counters.recordOutcome(success, slow));
    }

    public Optional<WindowState> tryClaim(ClaimParams params) {
        if (claimed >= params.permitted()) return Optional.empty();
        return Optional.of(claim());
    }

    public WindowOutcome decide(WindowState after, ClaimParams params) {
        boolean justDecided = counters.total() < params.permitted() && after.counters().total() >= params.permitted();
        if (!justDecided) return PENDING;
        return after.counters().shouldClose(params) ? CLOSE : REOPEN;
    }

    public static WindowState parse(String encoded) {
        String[] parts = encoded.split("\\|", 3);
        return new WindowState(parts[0], Integer.parseInt(parts[1]), Counters.parse(parts[2]));
    }

    @Override
    public String toString() {
        return generation + "|" + claimed + "|" + counters;
    }
}
