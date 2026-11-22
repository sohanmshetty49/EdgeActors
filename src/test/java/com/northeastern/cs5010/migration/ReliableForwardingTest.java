package com.northeastern.cs5010.migration;

import static org.junit.jupiter.api.Assertions.*;

import com.northeastern.cs5010.migration.HandoffMessage.Type;
import com.northeastern.cs5010.transport.LocationRegistry;
import com.northeastern.cs5010.transport.TCPTransport;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Forwarded messages survive a link that is down, and an actor that moves
 * back toward its sender still gets that sender's messages in order.
 */
class ReliableForwardingTest {

    static final class Node {
        final String name;
        final TCPTransport tcp;
        final FlakyLink link;
        final LocationRegistry registry = new LocationRegistry();
        final LocalActorHost host = new LocalActorHost(LocalCluster.factories());
        String address;
        MigrationManager manager;

        Node(String name) throws IOException {
            this.name = name;
            this.tcp = new TCPTransport(name, 0);
            tcp.start();
            this.link = new FlakyLink(tcp);
            this.address = "127.0.0.1:" + tcp.boundPort();
        }

        CounterActor counter(String id) {
            CounterActor copy = new CounterActor();
            copy.restore(host.snapshot(id));
            return copy;
        }
    }

    private final Map<String, Node> nodes = new LinkedHashMap<>();

    private void start(MigrationManager.Config config, String... names) throws IOException {
        for (String n : names) {
            nodes.put(n, new Node(n));
        }
        for (Node node : nodes.values()) {
            Map<String, String> peers = new HashMap<>();
            for (Node other : nodes.values()) {
                if (other != node) {
                    peers.put(other.name, other.address);
                }
            }
            node.manager = new MigrationManager(node.host, node.registry, node.link, peers, config);
        }
    }

    private Node node(String name) {
        return nodes.get(name);
    }

    @AfterEach
    void tearDown() {
        for (Node n : nodes.values()) {
            if (n.manager != null) {
                n.manager.close();
            }
            n.tcp.close();
        }
    }

    private static void await(BooleanSupplier condition, String what) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                fail("timed out waiting for " + what);
            }
            Thread.sleep(5);
        }
    }

    @Test
    void aForwardThatHitsADownLinkIsRetriedInOrderInsteadOfLost() throws Exception {
        start(LocalCluster.FAST, "a", "b", "c");
        Node a = node("a");
        Node b = node("b");
        Node c = node("c");
        c.manager.spawn("x", CounterActor.TYPE);
        a.manager.learn("x", "c", 1); // a knows the newer route
        b.manager.learn("x", "a", 0); // b still thinks x lives on a

        a.link.failWhen((dest, type) -> dest.equals(c.address)); // a cannot reach c
        for (int i = 1; i <= 5; i++) {
            b.manager.send("x", Integer.toString(i)); // reaches a, which must forward to c
        }
        await(() -> a.manager.queuedCount() == 5, "a to queue the five forwards");
        assertEquals(0, a.manager.droppedCount(), "nothing is dropped while the link is down");
        assertThrows(IOException.class, () -> a.manager.send("x", "99"),
            "a client's own send still fails visibly while the link is down");

        a.link.heal();
        a.manager.send("x", "6"); // goes out after the five waiting ones, never ahead of them
        await(() -> c.host.hosts("x") && c.counter("x").count() == 6, "all six messages to reach c");
        LocalCluster.assertInvariants(c.counter("x"), 6);
        assertEquals(0, a.manager.queuedCount());
        assertTrue(a.manager.retriedCount() >= 5, "retried " + a.manager.retriedCount());
    }

    @Test
    void protocolMessagesNeverOvertakeWaitingForwards() throws Exception {
        start(LocalCluster.FAST, "a", "b", "c");
        Node a = node("a");
        Node b = node("b");
        Node c = node("c");
        c.manager.spawn("x", CounterActor.TYPE);
        a.manager.learn("x", "c", 1);
        b.manager.learn("x", "a", 0);
        a.manager.spawn("y", CounterActor.TYPE);
        a.link.failWhen((dest, type) -> dest.equals(c.address) && type == Type.FORWARD);
        b.manager.send("x", "1");
        await(() -> a.manager.queuedCount() == 1, "the forward to wait");
        MigrationManager.Result r = a.manager.migrate("y", "c");
        assertEquals(MigrationManager.Outcome.ABORTED, r.outcome(), "PREPARE is not sent ahead of the waiting forward");
        assertTrue(a.host.hosts("y"));
        a.link.heal();
        await(() -> a.manager.queuedCount() == 0, "the forward to go out");
        assertTrue(a.manager.migrate("y", "c").committed());
    }

    @Test
    void anActorMovingBackToItsSenderGetsThatSendersMessagesInOrder() throws Exception {
        start(LocalCluster.FAST, "pi", "cloud");
        Node pi = node("pi");
        Node cloud = node("cloud");
        pi.manager.spawn("c", CounterActor.TYPE);
        assertTrue(pi.manager.migrate("c", "cloud").committed());
        await(() -> !cloud.manager.isDraining("c"), "cloud to finish draining");

        // cloud's DRAINED to pi is held up, as if the old node were slow to finish forwarding.
        cloud.link.failWhen((dest, type) -> dest.equals(pi.address) && type == Type.DRAINED);
        assertTrue(cloud.manager.migrate("c", "pi").committed());
        assertTrue(pi.manager.isDraining("c"));

        // Messages 1..3 were sent earlier and are still travelling through cloud:
        // cloud forwards them to pi after the move.
        long epoch = pi.registry.getEpoch("c");
        for (int i = 1; i <= 3; i++) {
            cloud.tcp.send(pi.address, HandoffMessage.forward("c", epoch, Integer.toString(i)).encode());
        }
        // Messages 4..6 are sent on pi itself after the actor arrived: they must wait.
        for (int i = 4; i <= 6; i++) {
            pi.manager.send("c", Integer.toString(i));
        }
        await(() -> pi.counter("c").count() == 3, "the forwarded messages");
        assertEquals(3, pi.counter("c").count(), "pi's own messages are held until cloud has drained");

        cloud.link.heal(); // DRAINED goes out on the next retry
        await(() -> !pi.manager.isDraining("c"), "pi to receive DRAINED");
        await(() -> pi.counter("c").count() == 6, "the held messages");
        LocalCluster.assertInvariants(pi.counter("c"), 6);
    }

    @Test
    void heldMessagesAreReleasedIfDrainedNeverArrives() throws Exception {
        MigrationManager.Config shortDrain = new MigrationManager.Config(
            Duration.ofSeconds(1), 3, Duration.ofMillis(200), Duration.ofSeconds(10), Duration.ofMillis(400));
        start(shortDrain, "pi", "cloud");
        Node pi = node("pi");
        Node cloud = node("cloud");
        pi.manager.spawn("c", CounterActor.TYPE);
        cloud.link.failWhen((dest, type) -> type == Type.DRAINED); // never gets through
        assertTrue(pi.manager.migrate("c", "cloud").committed());
        // pi's DRAINED to cloud is fine; now move it back with cloud's DRAINED blocked.
        await(() -> !cloud.manager.isDraining("c"), "cloud to finish draining");
        assertTrue(cloud.manager.migrate("c", "pi").committed());
        pi.manager.send("c", "1");
        assertTrue(pi.manager.isDraining("c"));
        MigrationManager.Result r = pi.manager.migrate("c", "cloud");
        assertEquals(MigrationManager.Outcome.ABORTED, r.outcome(), "cannot move on while still draining");
        assertTrue(r.reason().contains("still receiving messages forwarded from cloud"), r.reason());
        await(() -> !pi.manager.isDraining("c"), "the drain timeout");
        await(() -> pi.counter("c").count() == 1, "the held message");
    }

    @Test
    void configKeepsTheFourArgumentForm() {
        MigrationManager.Config c = new MigrationManager.Config(Duration.ofSeconds(1), 2, Duration.ofSeconds(1),
            Duration.ofSeconds(1));
        assertEquals(MigrationManager.Config.DEFAULT_DRAIN_TIMEOUT, c.drainTimeout());
        assertEquals(MigrationManager.Config.DEFAULT_DRAIN_TIMEOUT, MigrationManager.Config.defaults().drainTimeout());
        assertThrows(NullPointerException.class, () -> new MigrationManager.Config(Duration.ofSeconds(1), 2,
            Duration.ofSeconds(1), Duration.ofSeconds(1), null));
    }
}
