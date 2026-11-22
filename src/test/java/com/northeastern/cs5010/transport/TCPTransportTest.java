package com.northeastern.cs5010.transport;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TCPTransportTest {
    private TCPTransport a;
    private TCPTransport b;

    @BeforeEach
    void setUp() throws Exception {
        a = new TCPTransport("nodeA", 9101);
        b = new TCPTransport("nodeB", 9102);
        a.start();
        b.start();
    }

    @AfterEach
    void tearDown() {
        a.close();
        b.close();
    }

    @Test
    void sendAndReceiveRoundTrip() throws Exception {
        a.send("127.0.0.1:9102", "hello edge");
        assertEquals("hello edge", b.receive("nodeA"));
    }

    @Test
    void messagesArriveInOrderPerSender() throws Exception {
        for (int i = 0; i < 50; i++) a.send("127.0.0.1:9102", "m" + i);
        for (int i = 0; i < 50; i++) assertEquals("m" + i, b.receive("nodeA"));
    }

    @Test
    void oversizedMessageRejected() {
        String big = "x".repeat(TCPTransport.MAX_FRAME + 1);
        assertThrows(IllegalArgumentException.class, () -> a.send("127.0.0.1:9102", big));
    }

    @Test
    void decoderHandlesFramesSplitAcrossReads() {
        byte[] f = TCPTransport.frame("nodeA", "split");
        TCPTransport.FrameDecoder d = new TCPTransport.FrameDecoder();
        d.feed(ByteBuffer.wrap(f, 0, 3));
        assertNull(d.next());
        d.feed(ByteBuffer.wrap(f, 3, f.length - 3));
        assertEquals("nodeA\nsplit", d.next());
    }

    @Test
    void decoderRejectsBadLength() {
        TCPTransport.FrameDecoder d = new TCPTransport.FrameDecoder();
        d.feed(ByteBuffer.wrap(new byte[] {0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff}));
        assertThrows(IllegalStateException.class, d::next);
    }

    @Test
    void closeStopsTransport() {
        a.close();
        assertFalse(a.isConnected());
    }

    @Test
    void countersTrackFrames() throws Exception {
        a.send("127.0.0.1:9102", "one");
        assertEquals("one", b.receive("nodeA"));
        assertEquals(1, a.framesSent());
        assertEquals(1, b.framesReceived());
    }

    @Test
    void reusesOutboundConnection() throws Exception {
        a.send("127.0.0.1:9102", "first");
        a.send("127.0.0.1:9102", "second");
        assertEquals("first", b.receive("nodeA"));
        assertEquals("second", b.receive("nodeA"));
    }

    @Test
    void receiveTimesOutWithNull() throws Exception {
        assertNull(b.receive("nobody")); // waits 5 seconds
    }

    @Test
    void badPeerIsDroppedAndTransportSurvives() throws Exception {
        try (java.net.Socket bad = new java.net.Socket("127.0.0.1", 9102)) {
            bad.getOutputStream().write(new byte[] {0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff});
            bad.getOutputStream().flush();
            Thread.sleep(200);
        }
        a.send("127.0.0.1:9102", "still alive");
        assertEquals("still alive", b.receive("nodeA"));
    }

    @Test
    void constructorRejectsNewlineInName() {
        assertThrows(IllegalArgumentException.class, () -> new TCPTransport("bad\nname", 9103));
    }

    @Test
    void closeBeforeStartIsSafe() {
        TCPTransport t = new TCPTransport("solo", 9104);
        t.close();
        assertFalse(t.isConnected());
    }

    @Test
    void sendToUnreachableNodeFails() {
        assertThrows(IOException.class, () -> a.send("127.0.0.1:9199", "nobody home"));
    }
}
