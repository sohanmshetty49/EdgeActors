package com.northeastern.cs5010.transport;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** The single-JVM transport from Phase 2, used when no network is needed. */
class LocalTransportTest {

    @Test
    void deliversToTheNamedNodeInOrder() throws Exception {
        LocalTransport a = new LocalTransport("local-a");
        LocalTransport b = new LocalTransport("local-b");
        a.send("local-b", "first");
        a.send("local-b", "second");
        assertEquals("first", b.receive("local-a"));
        assertEquals("second", b.receive("local-a"));
    }

    @Test
    void sendToUnknownNodeIsIgnored() throws Exception {
        LocalTransport a = new LocalTransport("local-c");
        a.send("nobody-here", "lost");
        assertTrue(a.isConnected());
    }

    @Test
    void closeDisconnects() {
        LocalTransport a = new LocalTransport("local-d");
        a.close();
        assertFalse(a.isConnected());
    }

    @Test
    void interruptedSendKeepsTheInterruptFlag() {
        LocalTransport a = new LocalTransport("local-e");
        new LocalTransport("local-f");
        Thread.currentThread().interrupt();
        a.send("local-f", "x");
        assertTrue(Thread.interrupted(), "send must restore the interrupt flag"); // also clears it
    }
}
