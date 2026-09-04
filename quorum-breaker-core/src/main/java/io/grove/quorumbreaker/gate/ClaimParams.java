package io.grove.quorumbreaker.gate;

public record ClaimParams(int permitted, float failureRateThreshold, float slowCallRateThreshold) {
}
