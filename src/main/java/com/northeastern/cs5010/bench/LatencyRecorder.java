package com.northeastern.cs5010.bench;

import java.util.Arrays;

/**
 * Collects latency samples in nanoseconds and reports percentiles using the
 * nearest-rank method. Not thread-safe: use one per thread, then merge.
 */
public final class LatencyRecorder {

    private long[] samples = new long[1024];
    private int size;

    public void record(long nanos) {
        if (size == samples.length) {
            samples = Arrays.copyOf(samples, size * 2);
        }
        samples[size++] = nanos;
    }

    public void addAll(LatencyRecorder other) {
        for (int i = 0; i < other.size; i++) {
            record(other.samples[i]);
        }
    }

    public int count() {
        return size;
    }

    /** The p-th percentile (0 < p <= 100) in microseconds, or 0 with no samples. */
    public double percentileMicros(double p) {
        if (!(p > 0 && p <= 100)) {
            throw new IllegalArgumentException("percentile must be in (0, 100]");
        }
        if (size == 0) {
            return 0;
        }
        long[] sorted = Arrays.copyOf(samples, size);
        Arrays.sort(sorted);
        int rank = (int) Math.ceil(p / 100.0 * size);
        return sorted[Math.max(rank, 1) - 1] / 1_000.0;
    }
}
