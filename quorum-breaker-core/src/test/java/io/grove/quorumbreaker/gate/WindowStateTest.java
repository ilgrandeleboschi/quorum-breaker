package io.grove.quorumbreaker.gate;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WindowStateTest {

    @Test
    void fresh_startsAtZeroClaimedWithEmptyCounters() {
        WindowState state = WindowState.fresh("gen-1");

        assertEquals(0, state.claimed());
        assertEquals(new Counters(0, 0, 0), state.counters());
    }

    @Test
    void tryClaim_belowPermitted_incrementsClaimed() {
        WindowState state = WindowState.fresh("gen-1");
        ClaimParams params = new ClaimParams(2, 50f, 100f);

        Optional<WindowState> next = state.tryClaim(params);

        assertTrue(next.isPresent());
        assertEquals(1, next.get().claimed());
    }

    @Test
    void tryClaim_atPermitted_isDenied() {
        WindowState state = WindowState.fresh("gen-1").claim().claim();
        ClaimParams params = new ClaimParams(2, 50f, 100f);

        assertEquals(Optional.empty(), state.tryClaim(params));
    }

    @Test
    void release_decrementsClaimed_withoutTouchingCounters() {
        WindowState claimed = WindowState.fresh("gen-1").claim().recordOutcome(true, false);

        WindowState released = claimed.release();

        assertEquals(0, released.claimed());
        assertEquals(claimed.counters(), released.counters());
    }

    @Test
    void decide_belowPermittedTotal_isPending() {
        WindowState before = WindowState.fresh("gen-1");
        WindowState after = before.recordOutcome(true, false);
        ClaimParams params = new ClaimParams(2, 50f, 100f);

        assertEquals(WindowOutcome.PENDING, before.decide(after, params));
    }

    @Test
    void decide_crossingPermittedTotal_withLowFailureRate_isClose() {
        WindowState before = WindowState.fresh("gen-1").recordOutcome(true, false);
        WindowState after = before.recordOutcome(true, false);
        ClaimParams params = new ClaimParams(2, 50f, 100f);

        assertEquals(WindowOutcome.CLOSE, before.decide(after, params));
    }

    @Test
    void decide_crossingPermittedTotal_withHighFailureRate_isReopen() {
        WindowState before = WindowState.fresh("gen-1").recordOutcome(false, false);
        WindowState after = before.recordOutcome(false, false);
        ClaimParams params = new ClaimParams(2, 50f, 100f);

        assertEquals(WindowOutcome.REOPEN, before.decide(after, params));
    }

    @Test
    void decide_alreadyPastPermittedTotal_neverDecidesAgain() {
        WindowState before = WindowState.fresh("gen-1")
                .recordOutcome(true, false).recordOutcome(true, false);
        WindowState after = before.recordOutcome(true, false);
        ClaimParams params = new ClaimParams(2, 50f, 100f);

        assertEquals(WindowOutcome.PENDING, before.decide(after, params),
                "a window that already crossed the threshold must not decide a second time");
    }

    @Test
    void toString_andParse_roundTrip() {
        WindowState original = WindowState.fresh("gen-42").claim().recordOutcome(true, true);

        assertEquals(original, WindowState.parse(original.toString()));
    }

}
