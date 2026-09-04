package io.grove.quorumbreaker.gate;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CountersTest {

    @Test
    void recordOutcome_success_incrementsSuccessOnly() {
        Counters after = new Counters(0, 0, 0).recordOutcome(true, false);

        assertEquals(new Counters(1, 0, 0), after);
    }

    @Test
    void recordOutcome_failure_incrementsFailOnly() {
        Counters after = new Counters(0, 0, 0).recordOutcome(false, false);

        assertEquals(new Counters(0, 1, 0), after);
    }

    @Test
    void recordOutcome_slow_incrementsSlowIndependentlyOfOutcome() {
        Counters afterSlowSuccess = new Counters(0, 0, 0).recordOutcome(true, true);
        Counters afterSlowFailure = new Counters(0, 0, 0).recordOutcome(false, true);

        assertEquals(new Counters(1, 0, 1), afterSlowSuccess);
        assertEquals(new Counters(0, 1, 1), afterSlowFailure);
    }

    @Test
    void total_isSuccessPlusFail_slowIsNotCountedSeparately() {
        Counters counters = new Counters(3, 2, 4);

        assertEquals(5, counters.total());
    }

    @Test
    void shouldClose_belowBothThresholds_isTrue() {
        ClaimParams params = new ClaimParams(10, 50f, 50f);
        Counters counters = new Counters(8, 2, 1); // 20% fail, 10% slow

        assertTrue(counters.shouldClose(params));
    }

    @Test
    void shouldClose_failureRateExactlyAtThreshold_isFalse() {
        ClaimParams params = new ClaimParams(10, 50f, 100f);
        Counters counters = new Counters(5, 5, 0); // exactly 50% fail

        assertFalse(counters.shouldClose(params), "a rate exactly at the threshold must not count as healthy");
    }

    @Test
    void shouldClose_slowRateAtOrAboveThreshold_isFalse_evenWithNoFailures() {
        ClaimParams params = new ClaimParams(10, 100f, 50f);
        Counters counters = new Counters(10, 0, 5); // 0% fail, 50% slow

        assertFalse(counters.shouldClose(params), "slow calls alone must be able to keep the breaker open");
    }

    @Test
    void toString_andParse_roundTrip() {
        Counters original = new Counters(3, 1, 2);

        assertEquals(original, Counters.parse(original.toString()));
    }

}
