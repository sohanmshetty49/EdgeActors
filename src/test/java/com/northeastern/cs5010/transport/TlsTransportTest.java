package com.northeastern.cs5010.transport;

import static org.junit.jupiter.api.Assertions.*;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import javax.net.ssl.SSLSocket;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Mutual TLS transport: delivery, ordering, and who gets rejected. */
class TlsTransportTest {

    private TlsTransport a;
    private TlsTransport b;
    private String bAddress;

    @BeforeEach
    void setUp() throws Exception {
        a = new TlsTransport("nodeA", 0, TlsFixtures.context("cluster.p12"));
        b = new TlsTransport("nodeB", 0, TlsFixtures.context("cluster.p12"));
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
    void roundTripOverMutualTls() throws Exception {
        a.send(bAddress, "hello over tls");
        assertEquals("hello over tls", b.receive("nodeA"));
        assertEquals(1, a.framesSent());
        assertEquals(1, b.framesReceived());
    }

    @Test
    void messagesArriveInOrder() throws Exception {
        for (int i = 0; i < 100; i++) {
            a.send(bAddress, "m" + i);
        }
        for (int i = 0; i < 100; i++) {
            assertEquals("m" + i, b.receive("nodeA", 2_000));
        }
    }

    @Test
    void listenerReceivesInsteadOfInbox() throws Exception {
        List<String> seen = new CopyOnWriteArrayList<>();
        b.setListener((sender, payload) -> seen.add(sender + ":" + payload));
        a.send(bAddress, "pushed");
        long deadline = System.currentTimeMillis() + 3_000;
        while (seen.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertEquals(List.of("nodeA:pushed"), seen);
        assertNull(b.receive("nodeA", 100));
    }

    @Test
    void untrustedServerIsRejectedBySender() throws Exception {
        TlsTransport outsider = new TlsTransport("outsider", 0, TlsFixtures.context("outsider.p12"));
        outsider.start();
        try {
            assertThrows(IOException.class, () -> a.send("127.0.0.1:" + outsider.boundPort(), "secret"));
        } finally {
            outsider.close();
        }
    }

    @Test
    void untrustedClientNeverDelivers() throws Exception {
        TlsTransport intruder = new TlsTransport("intruder", 0, TlsFixtures.context("intruder.p12"));
        try {
            intruder.send(bAddress, "let me in");
        } catch (IOException expected) {
            // depending on timing the rejection surfaces on the intruder's side or only on b's
        } finally {
            intruder.close();
        }
        assertNull(b.receive("intruder", 1_000));
        a.send(bAddress, "still serving members");
        assertEquals("still serving members", b.receive("nodeA", 2_000));
    }

    @Test
    void plainTcpClientIsDroppedAndServerSurvives() throws Exception {
        try (Socket raw = new Socket("127.0.0.1", b.boundPort())) {
            raw.getOutputStream().write("not a tls handshake".getBytes());
            raw.getOutputStream().flush();
            Thread.sleep(200);
        }
        a.send(bAddress, "after junk");
        assertEquals("after junk", b.receive("nodeA", 2_000));
    }

    @Test
    void memberSendingBadLengthIsDroppedAndServerSurvives() throws Exception {
        for (int badLength : new int[] {-1, FramedTransport.MAX_FRAME + 1}) {
            SSLSocket member = (SSLSocket) TlsFixtures.context("cluster.p12").getSocketFactory()
                .createSocket("127.0.0.1", b.boundPort());
            try (member; DataOutputStream out = new DataOutputStream(member.getOutputStream())) {
                member.startHandshake();
                out.writeInt(badLength);
                out.flush();
                // The server closes the connection, so the next read sees end of stream.
                assertEquals(-1, member.getInputStream().read());
            }
        }
        a.send(bAddress, "still serving");
        assertEquals("still serving", b.receive("nodeA", 2_000));
        assertEquals(1, b.framesReceived());
    }

    @Test
    void oversizedMessageRejected() {
        String big = "x".repeat(FramedTransport.MAX_FRAME + 1);
        assertThrows(IllegalArgumentException.class, () -> a.send(bAddress, big));
    }

    @Test
    void unreachablePeerFails() {
        assertThrows(IOException.class, () -> a.send("127.0.0.1:1", "nobody"));
    }

    @Test
    void reconnectsAfterPeerRestart() throws Exception {
        a.send(bAddress, "before");
        assertEquals("before", b.receive("nodeA", 2_000));
        int port = b.boundPort();
        b.close();
        b = new TlsTransport("nodeB", port, TlsFixtures.context("cluster.p12"));
        b.start();
        // The pooled connection is dead; a send may fail once, then must reconnect.
        String got = null;
        for (int i = 0; i < 10 && got == null; i++) {
            try {
                a.send(bAddress, "after");
            } catch (IOException expected) {
                // broken pooled socket detected, next attempt reconnects
            }
            got = b.receive("nodeA", 300);
        }
        assertEquals("after", got);
    }

    @Test
    void closeIsIdempotentAndStopsTransport() {
        a.close();
        a.close();
        assertFalse(a.isConnected());
        assertTrue(b.isConnected());
    }

    @Test
    void requiresContext() {
        assertThrows(IllegalArgumentException.class, () -> new TlsTransport("x", 0, null));
    }

    @Test
    void wrongKeystorePasswordFails() throws Exception {
        Path store = TlsFixtures.dir().resolve("cluster.p12");
        assertThrows(IOException.class, () -> TlsContexts.fromKeystore(store, "wrong".toCharArray()));
    }
}
