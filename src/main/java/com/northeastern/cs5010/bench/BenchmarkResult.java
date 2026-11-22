package com.northeastern.cs5010.bench;

/**
 * The outcome of running one workload against one concurrency model.
 * Elapsed time runs from the moment all clients start until the final total
 * is returned, so queued work in asynchronous models is included.
 */
public record BenchmarkResult(String model, WorkloadSpec spec, long writes, long reads, long elapsedNanos,
                              long actualTotal, double readP50Micros, double readP95Micros,
                              double readP99Micros) {

    public long operations() {
        return writes + reads;
    }

    public double throughputOpsPerSec() {
        return operations() / (elapsedNanos / 1e9);
    }

    /** Every write was applied exactly once: no lost and no duplicated updates. */
    public boolean correct() {
        return actualTotal == writes;
    }
}
