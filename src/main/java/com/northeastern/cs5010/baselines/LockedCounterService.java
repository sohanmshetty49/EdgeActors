package com.northeastern.cs5010.baselines;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Baseline 1: shared memory with threads and locks.
 *
 * <p>Counters live in a plain array. Keys are split into stripes, each guarded
 * by its own lock, so clients updating different stripes do not block each
 * other. Work runs directly on the caller's thread: no queues, no handoff.
 * This is the classic approach the actor model is compared against.
 */
public final class LockedCounterService implements CounterService {

    private final long[] counts;
    private final ReentrantLock[] stripes;

    public LockedCounterService(int keys, int stripeCount) {
        if (keys < 1 || stripeCount < 1) {
            throw new IllegalArgumentException("keys and stripes must be at least 1");
        }
        counts = new long[keys];
        stripes = new ReentrantLock[stripeCount];
        for (int i = 0; i < stripeCount; i++) {
            stripes[i] = new ReentrantLock();
        }
    }

    @Override
    public String name() {
        return "threads+locks";
    }

    @Override
    public void increment(int key) {
        ReentrantLock lock = stripeFor(key);
        lock.lock();
        try {
            counts[key]++;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long get(int key) {
        ReentrantLock lock = stripeFor(key);
        lock.lock();
        try {
            return counts[key];
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long total() {
        // Lock every stripe in a fixed order so totals are consistent and
        // concurrent totals cannot deadlock.
        for (ReentrantLock lock : stripes) {
            lock.lock();
        }
        try {
            long sum = 0;
            for (long c : counts) {
                sum += c;
            }
            return sum;
        } finally {
            for (int i = stripes.length - 1; i >= 0; i--) {
                stripes[i].unlock();
            }
        }
    }

    private ReentrantLock stripeFor(int key) {
        if (key < 0 || key >= counts.length) {
            throw new IllegalArgumentException("key out of range: " + key);
        }
        return stripes[key % stripes.length];
    }

    @Override
    public void close() {
        // nothing to release: no threads of its own
    }
}
