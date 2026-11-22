package com.northeastern.cs5010.transport;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Listener mode, address parsing and failure paths shared by both transports. */
class FramedTransportTest {

    private TCPTransport a;
    private TCPTransport b;
    private String bAddress;

    @BeforeEach
    void setUp() throws Exception {
        a = new TCPTransport("nodeA", 0);
        b = new TCPTransport("nodeB", 0);
        a.start();
        b.start();
        bAddress = "127.0.0.1:" + b.boundPort();
    }

    @AfterEach
    void tearDown() {
        a.close();
        b.close();
    }

    @Test
    void portZeroBindsAFreePort() throws Exception {
        assertTrue(b.boundPort() > 0);
        assertEquals("nodeB", b.nodeName());
    }

    @Test
    void listenerGetsSenderAndPayload() throws Exception {
        List<String> seen = new CopyOnWriteArrayList<>();
        b.setListener((sender, payload) -> seen.add(sender + "|" + payload));
        a.send(bAddress, "multi\nline payload");
        awaitSize(seen, 1);
        assertEquals("nodeA|multi\nline payload", seen.get(0));
    }

    @Test
    void failingListenerDoesNotKillTheConnection() throws Exception {
        List<String> seen = new CopyOnWriteArrayList<>();
        b.setListener((sender, payload) -> {
            if (payload.equals("boom")) {
                throw new IllegalStateException("listener bug");
            }
            seen.add(payload);
        });
        a.send(bAddress, "boom");
        a.send(bAddress, "fine");
        awaitSize(seen, 1);
        assertEquals("fine", seen.get(0));
    }

    @Test
    void clearingListenerReturnsToPolling() throws Exception {
        b.setListener((sender, payload) -> { });
        b.setListener(null);
        a.send(bAddress, "polled");
        assertEquals("polled", b.receive("nodeA", 2_000));
    }

    @Test
    void frameWithoutSenderLineDropsOnlyThatConnection() throws Exception {
        byte[] body = "no sender line".getBytes();
        try (Socket raw = new Socket("127.0.0.1", b.boundPort())) {
            OutputStream out = raw.getOutputStream();
            out.write(ByteBuffer.allocate(4).putInt(body.length).array());
            out.write(body);
            out.flush();
            Thread.sleep(200);
        }
        a.send(bAddress, "still fine");
        assertEquals("still fine", b.receive("nodeA", 2_000));
        assertEquals(1, b.framesReceived());
    }

    @Test
    void reconnectsAfterPeerRestart() throws Exception {
        a.send(bAddress, "before");
        assertEquals("before", b.receive("nodeA", 2_000));
        int port = b.boundPort();
        b.close();
        b = new TCPTransport("nodeB", port);
        b.start();
        String got = null;
        for (int i = 0; i < 20 && got == null; i++) {
            try {
                a.send(bAddress, "after");
            } catch (IOException expected) {
                // the pooled channel was dead; the next send reconnects
            }
            got = b.receive("nodeA", 200);
        }
        assertEquals("after", got);
    }

    @Test
    void addressParsing() {
        assertEquals(new FramedTransport.HostPort("10.0.0.5", 9100), FramedTransport.parseAddress("10.0.0.5:9100"));
        assertEquals(new FramedTransport.HostPort("::1", 80), FramedTransport.parseAddress("::1:80"));
        for (String bad : new String[] {null, "nohost", ":9100", "host:", "host:abc", "host:0", "host:70000"}) {
            assertThrows(IllegalArgumentException.class, () -> FramedTransport.parseAddress(bad), String.valueOf(bad));
        }
    }

    @Test
    void sendToMalformedAddressFails() {
        assertThrows(IllegalArgumentException.class, () -> a.send("not-an-address", "x"));
    }

    @Test
    void rejectsBadNodeNames() {
        assertThrows(IllegalArgumentException.class, () -> new TCPTransport("", 0));
        assertThrows(IllegalArgumentException.class, () -> new TCPTransport(null, 0));
    }

    @Test
    void decoderReturnsSeveralFramesFromOneRead() {
        byte[] f1 = FramedTransport.frame("n", "one");
        byte[] f2 = FramedTransport.frame("n", "two");
        FramedTransport.FrameDecoder d = new FramedTransport.FrameDecoder();
        d.feed(ByteBuffer.allocate(f1.length + f2.length).put(f1).put(f2).flip());
        assertEquals("n\none", d.next());
        assertEquals("n\ntwo", d.next());
        assertNull(d.next());
    }

    @Test
    void decoderWaitsWhenTheBodyArrivesInPieces() {
        byte[] f = FramedTransport.frame("n", "body split mid-way");
        FramedTransport.FrameDecoder d = new FramedTransport.FrameDecoder();
        d.feed(ByteBuffer.wrap(f, 0, 6)); // full length prefix, partial body
        assertNull(d.next());
        d.feed(ByteBuffer.wrap(f, 6, f.length - 6));
        assertEquals("n\nbody split mid-way", d.next());
    }

    @Test
    void decoderRejectsNegativeLength() {
        FramedTransport.FrameDecoder d = new FramedTransport.FrameDecoder();
        d.feed(ByteBuffer.wrap(new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff}));
        assertThrows(IllegalStateException.class, d::next);
    }

    private static void awaitSize(List<String> list, int size) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 3_000;
        while (list.size() < size && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertEquals(size, list.size());
    }
}
