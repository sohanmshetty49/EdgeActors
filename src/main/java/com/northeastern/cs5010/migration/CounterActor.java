package com.northeastern.cs5010.migration;

/**
 * Example migratable actor used by the tests and the Pi demo.
 *
 * <p>It counts every message and sums the numeric ones. When a sender numbers
 * its messages 1, 2, 3, ..., the state proves the three handoff invariants on
 * its own: count equals the number sent (no loss), sum equals n(n+1)/2 (no
 * duplication), and outOfOrder stays 0 (per-sender order kept).
 */
public final class CounterActor implements MigratableActor {

    public static final String TYPE = "counter";

    private long count;
    private long sum;
    private long last;
    private long outOfOrder;

    @Override
    public void onMessage(String payload) {
        count++;
        long value;
        try {
            value = Long.parseLong(payload.trim());
        } catch (NumberFormatException e) {
            return; // counted, but not part of the numbered sequence
        }
        sum += value;
        if (value <= last) {
            outOfOrder++;
        }
        last = value;
    }

    @Override
    public String snapshot() {
        return count + "," + sum + "," + last + "," + outOfOrder;
    }

    @Override
    public void restore(String state) {
        String[] parts = state.split(",");
        if (parts.length != 4) {
            throw new IllegalArgumentException("bad counter state: " + state);
        }
        try {
            count = Long.parseLong(parts[0]);
            sum = Long.parseLong(parts[1]);
            last = Long.parseLong(parts[2]);
            outOfOrder = Long.parseLong(parts[3]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("bad counter state: " + state, e);
        }
    }

    public long count() {
        return count;
    }

    public long sum() {
        return sum;
    }

    public long outOfOrder() {
        return outOfOrder;
    }
}
