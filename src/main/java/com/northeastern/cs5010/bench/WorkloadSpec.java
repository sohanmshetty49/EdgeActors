package com.northeastern.cs5010.bench;

/**
 * What a benchmark run does: how many client threads, how many operations
 * each, how many keys, what share are reads, and how skewed key access is.
 *
 * <p>Skew: hotTrafficFraction of operations go to the first hotKeyFraction of
 * the keys. For example 0.2 and 0.8 sends 80% of traffic to 20% of keys. With
 * hotKeyFraction 1.0 access is uniform.
 *
 * @param seed makes the operation streams reproducible across runs and machines
 */
public record WorkloadSpec(int clients, int opsPerClient, int keys, double readRatio,
                           double hotKeyFraction, double hotTrafficFraction, long seed) {

    public WorkloadSpec {
        if (clients < 1 || opsPerClient < 1 || keys < 1) {
            throw new IllegalArgumentException("clients, ops and keys must be at least 1");
        }
        checkFraction("readRatio", readRatio);
        checkFraction("hotTrafficFraction", hotTrafficFraction);
        if (!(hotKeyFraction > 0 && hotKeyFraction <= 1)) {
            throw new IllegalArgumentException("hotKeyFraction must be in (0, 1]");
        }
    }

    /** Uniform key access. */
    public static WorkloadSpec uniform(int clients, int opsPerClient, int keys, double readRatio, long seed) {
        return new WorkloadSpec(clients, opsPerClient, keys, readRatio, 1.0, 1.0, seed);
    }

    public long totalOps() {
        return (long) clients * opsPerClient;
    }

    /** Number of keys in the hot set, at least 1. */
    public int hotKeys() {
        return Math.max(1, (int) Math.round(keys * hotKeyFraction));
    }

    private static void checkFraction(String name, double value) {
        if (!(value >= 0 && value <= 1)) {
            throw new IllegalArgumentException(name + " must be in [0, 1]");
        }
    }
}
