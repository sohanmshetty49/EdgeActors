package com.northeastern.cs5010.bench;

import static org.junit.jupiter.api.Assertions.*;

import com.northeastern.cs5010.migration.HandoffMessage;
import com.northeastern.cs5010.transport.AsyncTransport;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.Test;

/** Network fault injection: delay, lost protocol messages, partitions, and the invariants under them. */
class NetworkFaultTest {

    /** Records what reaches the real network. */
    static final class Recording implements AsyncTransport {
        final List<String> sent = new ArrayList<>();

        @Override
        public synchronized void send(String destination, String message) {
            sent.add(message);
        }

        @Override
        public String receive(String source) {
            return null;
        }

        @Override
        public boolean isConnected() {
            return true;
        }

        @Override
        public void close() {
        }

        @Override
        public void start() {
        }

        @Override
        public String nodeName() {
            return "rec";
        }

        @Override
        public void setListener(BiConsumer<String, String> listener) {
        }
    }

    @Test
    void partitionRefusesSendsInBothDirectionsUntilHealed() throws Exception {
        ChaosTransport.Network net = new ChaosTransport.Network();
        Recording a = new Recording();
        Recording b = new Recording();
        ChaosTransport toB = new ChaosTransport(a, net, 0, 0, 1);
        ChaosTransport toA = new ChaosTransport(b, net, 0, 0, 2);
        net.partition();
        net.partition(); // already cut: still one partition
        assertTrue(net.isPartitioned());
        assertThrows(IOException.class, () -> toB.send("b", "hello"));
        assertThrows(IOException.class, () -> toA.send("a", "hello"));
        net.heal();
        toB.send("b", "hello");
        assertEquals(List.of("hello"), a.sent);
        assertEquals(1, net.partitionCount());
        assertEquals(1, toB.refusedCount());
        assertEquals(1, toA.refusedCount());
    }

    @Test
    void onlyRequestsAndRepliesAreDropped() throws Exception {
        Recording inner = new Recording();
        ChaosTransport chaos = new ChaosTransport(inner, new ChaosTransport.Network(), 0, 1.0, 3);
        chaos.send("x", HandoffMessage.prepare("a", "counter", 0, "s").encode());
        chaos.send("x", HandoffMessage.committed("a", 0).encode());
        chaos.send("x", HandoffMessage.query("a", 0).encode());
        String forward = HandoffMessage.forward("a", 1, "data").encode();
        String drained = HandoffMessage.drained("a", 0).encode();
        chaos.send("x", forward);
        chaos.send("x", drained);
        chaos.send("x", "plain text");
        assertEquals(List.of(forward, drained, "plain text"), inner.sent, "data stream messages always get through");
        assertEquals(3, chaos.droppedCount());
    }

    @Test
    void delaysAreCountedAndKeepOrder() throws Exception {
        Recording inner = new Recording();
        ChaosTransport chaos = new ChaosTransport(inner, new ChaosTransport.Network(), 2, 0, 4);
        for (int i = 0; i < 50; i++) {
            chaos.send("x", Integer.toString(i));
        }
        for (int i = 0; i < 50; i++) {
            assertEquals(Integer.toString(i), inner.sent.get(i));
        }
        assertTrue(chaos.delayedCount() > 0);
        assertEquals("rec", chaos.nodeName());
        assertTrue(chaos.isConnected());
    }

    @Test
    void chaosSettingsAreValidated() {
        Recording inner = new Recording();
        ChaosTransport.Network net = new ChaosTransport.Network();
        assertThrows(IllegalArgumentException.class, () -> new ChaosTransport(inner, net, -1, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> new ChaosTransport(inner, net, 0, 1.5, 1));
        assertThrows(NullPointerException.class, () -> new ChaosTransport(null, net, 0, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> new MigrationFaultHarness.Settings(0, 1, 0, 0, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> new MigrationFaultHarness.Settings(1, 0, 0, 0, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> new MigrationFaultHarness.Settings(1, 1, -1, 0, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> new MigrationFaultHarness.Settings(1, 1, 0, 2, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> new MigrationFaultHarness.Settings(1, 1, 0, 0, -1, 1));
    }

    @Test
    void recorderSeesLossDuplicationAndReordering() {
        SequenceRecorder r = new SequenceRecorder();
        for (String m : List.of("1", "2", "2", "5", "4")) {
            r.onMessage(m);
        }
        assertEquals(1, r.duplicates());
        assertEquals(2, r.outOfOrder(), "the repeated 2 and the late 4");
        assertEquals(4, r.seen().cardinality());
        SequenceRecorder copy = new SequenceRecorder();
        copy.restore(r.snapshot());
        assertEquals(r.snapshot(), copy.snapshot());
        assertEquals(r.seen(), copy.seen());
        assertThrows(IllegalArgumentException.class, () -> copy.restore("nonsense"));
    }

    @Test
    void invariantsHoldUnderDelayLostRepliesAndPartitions() throws Exception {
        MigrationFaultResult delay = MigrationFaultHarness.run(new MigrationFaultHarness.Settings(6, 200, 3, 0, 0, 11));
        MigrationFaultResult drops = MigrationFaultHarness.run(new MigrationFaultHarness.Settings(6, 200, 2, 0.15, 0, 12));
        MigrationFaultResult cuts = MigrationFaultHarness.run(new MigrationFaultHarness.Settings(8, 200, 2, 0.05, 1, 13));
        for (MigrationFaultResult r : List.of(delay, drops, cuts)) {
            assertTrue(r.invariantsHold(), r.toString());
            assertEquals(r.sent(), r.accepted() + r.reported(), "every send was either accepted or reported");
            assertEquals(r.accepted(), r.received(), "every accepted message arrived exactly once");
            assertTrue(r.committed() > 0, r.toString());
        }
        assertEquals(0, delay.reported(), "delay alone never fails a send");
        assertTrue(drops.droppedProtocol() > 0, drops.toString());
        assertEquals(8, cuts.partitions());
        assertTrue(cuts.refusedSends() > 0, cuts.toString());
    }

    @Test
    void benchmarkCliRunsTheNetworkFaultTable() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        int code = BenchmarkMain.run(new String[] {"--ops", "200", "--repeat", "1", "--warmup", "0", "--net-faults", "2"},
            new PrintStream(bytes, true, StandardCharsets.UTF_8));
        String out = bytes.toString(StandardCharsets.UTF_8);
        assertEquals(0, code, out);
        assertTrue(out.contains("Live migration under network faults (2 randomized runs per row"), out);
        assertTrue(out.contains("| delay up to 2 ms, 10% of protocol messages lost |"), out);
        assertTrue(out.contains("| Sends refused by partition |"), out);
        assertTrue(out.contains("partition in 100% of runs"), out);
        assertTrue(out.contains("All invariants held"), out);

        ByteArrayOutputStream bad = new ByteArrayOutputStream();
        assertEquals(2, BenchmarkMain.run(new String[] {"--net-faults", "-1"},
            new PrintStream(bad, true, StandardCharsets.UTF_8)));
    }
}
