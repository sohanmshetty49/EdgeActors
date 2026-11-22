package com.northeastern.cs5010.bench;

/**
 * Decides which jobs get an injected crash. The decision for a job depends
 * only on the seed and the job id, so a fault experiment can be replayed
 * exactly, on any machine.
 */
public record FaultPlan(double crashProbability, long seed) {

    public FaultPlan {
        if (!(crashProbability >= 0 && crashProbability <= 1)) {
            throw new IllegalArgumentException("crashProbability must be in [0, 1]");
        }
    }

    public static FaultPlan none() {
        return new FaultPlan(0, 0);
    }

    public boolean crashes(long jobId) {
        if (crashProbability == 0) {
            return false;
        }
        long h = mix(seed + jobId * 0x9E3779B97F4A7C15L);
        double u = (h >>> 11) * 0x1.0p-53; // uniform in [0, 1)
        return u < crashProbability;
    }

    /** SplitMix64 finalizer: spreads nearby inputs into unrelated outputs. */
    private static long mix(long z) {
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }
}
