package com.northeastern.cs5010.actors;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.LongSupplier;

/**
 * Sliding-window restart budget: allows at most maxRestarts restarts within
 * any window of the given length. Old restarts fall out of the window, so an
 * actor that fails rarely keeps being restarted, while one stuck in a crash
 * loop runs out of budget quickly.
 */
public final class RestartBudget {

    private final int maxRestarts;
    private final long windowNanos;
    private final LongSupplier clock;
    private final Deque<Long> recent = new ArrayDeque<>();

    /** clock returns nanoseconds; tests pass a fake clock to move time by hand. */
    public RestartBudget(int maxRestarts, Duration window, LongSupplier clock) {
        this.maxRestarts = maxRestarts;
        this.windowNanos = window.toNanos();
        this.clock = clock;
    }

    /** Records a restart and returns true, or returns false if the budget is used up. */
    public synchronized boolean tryAcquire() {
        long now = clock.getAsLong();
        while (!recent.isEmpty() && now - recent.peekFirst() >= windowNanos) {
            recent.pollFirst();
        }
        if (recent.size() >= maxRestarts) {
            return false;
        }
        recent.addLast(now);
        return true;
    }

    /** Restarts counted inside the current window. */
    public synchronized int used() {
        return recent.size();
    }
}
