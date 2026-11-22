package com.northeastern.cs5010.migration;

import static org.junit.jupiter.api.Assertions.*;

import com.northeastern.cs5010.migration.HandoffMessage.Type;
import com.northeastern.cs5010.migration.LocalCluster.Node;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Drives two nodes through the same commands used in the Pi demo. */
class NodeShellTest {

    private LocalCluster cluster;
    private NodeShell pi;
    private NodeShell cloud;

    @BeforeEach
    void setUp() throws Exception {
        cluster = LocalCluster.of("pi", "cloud");
        pi = shell(cluster.node("pi"));
        cloud = shell(cluster.node("cloud"));
    }

    @AfterEach
    void tearDown() {
        cluster.close();
    }

    @Test
    void demoScriptEndToEnd() throws Exception {
        assertEquals("spawned c (counter) on pi", pi.execute("spawn c"));
        assertEquals("sent 10 messages to c (1..10)", pi.execute("send c 10"));
        assertEquals("sent 5 messages to c (11..15)", pi.execute("send c 5"));
        assertTrue(pi.execute("status c").endsWith("count=15 sum=120 outOfOrder=0"), pi.execute("status c"));

        String moved = pi.execute("migrate c cloud");
        assertTrue(moved.startsWith("COMMITTED in "), moved);
        assertEquals("sent 5 messages to c (16..20)", pi.execute("send c 5"));
        assertEquals("c: location=cloud epoch=1 status=ACTIVE", pi.execute("status c"));

        cluster.node("cloud").awaitCount("c", 20);
        assertEquals("c: location=cloud epoch=1 status=ACTIVE count=20 sum=210 outOfOrder=0",
            cloud.execute("status c"));
    }

    @Test
    void tellSendsFreeText() throws Exception {
        pi.execute("spawn c");
        assertEquals("sent to c", pi.execute("tell c hello there world"));
        cluster.node("pi").awaitCount("c", 1);
    }

    @Test
    void statusShowsInDoubtAndResolveSettlesIt() throws Exception {
        Node cloudNode = cluster.node("cloud");
        pi.execute("spawn c");
        cloudNode.faulty.dropAll(Type.COMMITTED);
        assertTrue(pi.execute("migrate c cloud").startsWith("IN_DOUBT"));
        assertTrue(pi.execute("status c").contains("in-doubt (run resolve c)"), pi.execute("status c"));
        cloudNode.faulty.heal();
        assertTrue(pi.execute("resolve c").startsWith("COMMITTED"));
    }

    @Test
    void nonCounterActorsShowRawState() {
        pi.execute("spawn s slow");
        assertTrue(pi.execute("status s").endsWith("state=0"), pi.execute("status s"));
    }

    @Test
    void learnAndUnknownActors() {
        assertEquals("r is on cloud at epoch 3", pi.execute("learn r cloud 3"));
        assertEquals("r2 is on cloud at epoch 0", pi.execute("learn r2 cloud"));
        assertEquals("r: location=cloud epoch=3 status=ACTIVE", pi.execute("status r"));
        assertEquals("nobody is unknown on pi", pi.execute("status nobody"));
    }

    @Test
    void resolveWithoutDoubt() {
        pi.execute("spawn c");
        String out = pi.execute("resolve c");
        assertTrue(out.startsWith("ABORTED in ") && out.endsWith("(no in-doubt handoff for c)"), out);
    }

    @Test
    void reportsUsageAndInputErrors() {
        pi.execute("spawn c");
        assertEquals("", pi.execute("   "));
        assertTrue(pi.execute("help").contains("migrate <actor> <node>"));
        assertEquals("error: usage: migrate <actor> <node>", pi.execute("migrate c"));
        assertEquals("error: usage: send <actor> <count>", pi.execute("send c"));
        assertEquals("error: count must be at least 1", pi.execute("send c 0"));
        assertTrue(pi.execute("send c lots").startsWith("error: "));
        assertEquals("error: usage: tell <actor> <text>", pi.execute("tell c"));
        assertEquals("error: usage: status <actor>", pi.execute("status"));
        assertTrue(pi.execute("spawn c").startsWith("error: c already exists"));
        assertEquals("unknown command 'fly', type help", pi.execute("fly me"));
    }

    @Test
    void networkErrorsAreReported() {
        pi.execute("learn z cloud");
        cluster.node("cloud").transport.close();
        assertTrue(pi.execute("tell z hi").startsWith("network error: "));
    }

    @Test
    void statsShowCounters() {
        pi.execute("spawn c");
        pi.execute("migrate c cloud");
        String stats = pi.execute("stats");
        assertTrue(stats.startsWith("node=pi framesSent="), stats);
        assertTrue(stats.contains(" commits=1 aborts=0 "), stats);
        assertTrue(stats.contains(" cores="), stats);
    }

    private static NodeShell shell(Node node) {
        return new NodeShell(node.manager, node.host, node.registry, node.transport);
    }
}
