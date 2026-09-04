package io.grove.quorumbreaker.gate;

import io.grove.quorumbreaker.cluster.ClusterCoordinatorException;

import java.util.HashMap;
import java.util.Map;

public record Counters(int success, int fail, int slow) {

    public static Counters parse(String encoded) {
        try {
            Map<String, Integer> values = new HashMap<>();
            for (String field : encoded.split(",")) {
                String[] keyAndValue = field.split(":");
                values.put(keyAndValue[0], Integer.parseInt(keyAndValue[1]));
            }
            return new Counters(values.get("success"), values.get("fail"), values.get("slow"));
        } catch (RuntimeException malformed) {
            throw new ClusterCoordinatorException("corrupt quorum-breaker window state: '" + encoded + "'", malformed);
        }
    }

    public Counters recordOutcome(boolean success, boolean slow) {
        return new Counters(success ? this.success + 1 : this.success,
                success ? this.fail : this.fail + 1,
                slow ? this.slow + 1 : this.slow);
    }

    public int total() {
        return success + fail;
    }

    public boolean shouldClose(ClaimParams params) {
        boolean failureRateOk = (fail * 100.0) / total() < params.failureRateThreshold();
        boolean slowCallRateOk = (slow * 100.0) / total() < params.slowCallRateThreshold();
        return failureRateOk && slowCallRateOk;
    }

    @Override
    public String toString() {
        return "success:" + success + ",fail:" + fail + ",slow:" + slow;
    }
}
