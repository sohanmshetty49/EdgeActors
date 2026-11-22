package com.northeastern.cs5010.baselines;

/**
 * The shared workload every concurrency model implements: a set of keyed
 * counters that many client threads update and read at the same time.
 *
 * <p>Having the actor model and the two baselines behind one interface means
 * the benchmark runs the exact same operations against each of them, so the
 * numbers compare the concurrency models and nothing else.
 */
public interface CounterService extends AutoCloseable {

    /** Short name used in benchmark reports. */
    String name();

    /** Adds one to the counter for key. May return before the update is applied. */
    void increment(int key) throws InterruptedException;

    /** Returns the current value for key, waiting for the answer (a round trip). */
    long get(int key) throws InterruptedException;

    /**
     * Returns the sum over all keys. Includes every increment whose call
     * returned before this call started.
     */
    long total() throws InterruptedException;

    /** Releases threads. Safe to call more than once. */
    @Override
    void close();
}
