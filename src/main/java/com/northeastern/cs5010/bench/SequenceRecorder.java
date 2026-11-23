package com.northeastern.cs5010.bench;

import com.northeastern.cs5010.migration.MigratableActor;
import java.util.Base64;
import java.util.BitSet;

/**
 * Migratable actor that records exactly which numbered messages it received,
 * so a fault run can tell loss, duplication, and reordering apart. Its whole
 * record moves with it on migration.
 */
public final class SequenceRecorder implements MigratableActor {

    public static final String TYPE = "recorder";

    private BitSet seen = new BitSet();
    private long duplicates;
    private long outOfOrder;
    private long last;

    @Override
    public void onMessage(String payload) {
        int seq = Integer.parseInt(payload.trim());
        if (seen.get(seq)) {
            duplicates++;
        }
        seen.set(seq);
        if (seq <= last) {
            outOfOrder++;
        }
        last = seq;
    }

    @Override
    public String snapshot() {
        return duplicates + "," + outOfOrder + "," + last + "," + Base64.getEncoder().encodeToString(seen.toByteArray());
    }

    @Override
    public void restore(String state) {
        String[] parts = state.split(",", -1);
        if (parts.length != 4) {
            throw new IllegalArgumentException("bad recorder state");
        }
        duplicates = Long.parseLong(parts[0]);
        outOfOrder = Long.parseLong(parts[1]);
        last = Long.parseLong(parts[2]);
        seen = BitSet.valueOf(Base64.getDecoder().decode(parts[3]));
    }

    /** A copy of the numbers received so far. */
    public BitSet seen() {
        return (BitSet) seen.clone();
    }

    public long duplicates() {
        return duplicates;
    }

    public long outOfOrder() {
        return outOfOrder;
    }
}
