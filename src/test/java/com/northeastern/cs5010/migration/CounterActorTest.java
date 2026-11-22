package com.northeastern.cs5010.migration;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** The example actor whose state proves the handoff invariants. */
class CounterActorTest {

    @Test
    void countsSumsAndDetectsReordering() {
        CounterActor c = new CounterActor();
        c.onMessage("1");
        c.onMessage("2");
        c.onMessage("2"); // duplicate
        c.onMessage("1"); // out of order
        assertEquals(4, c.count());
        assertEquals(6, c.sum());
        assertEquals(2, c.outOfOrder());
    }

    @Test
    void nonNumericMessagesAreCountedOnly() {
        CounterActor c = new CounterActor();
        c.onMessage("hello");
        c.onMessage(" 5 ");
        assertEquals(2, c.count());
        assertEquals(5, c.sum());
        assertEquals(0, c.outOfOrder());
    }

    @Test
    void snapshotRestoreRoundTrip() {
        CounterActor a = new CounterActor();
        for (int i = 1; i <= 10; i++) {
            a.onMessage(Integer.toString(i));
        }
        CounterActor b = new CounterActor();
        b.restore(a.snapshot());
        assertEquals(a.snapshot(), b.snapshot());
        b.onMessage("11");
        assertEquals(0, b.outOfOrder(), "restore keeps the last value, so order checking continues");
    }

    @Test
    void restoreRejectsBadState() {
        CounterActor c = new CounterActor();
        assertThrows(IllegalArgumentException.class, () -> c.restore("1,2,3"));
        assertThrows(IllegalArgumentException.class, () -> c.restore("1,2,x,4"));
    }
}
