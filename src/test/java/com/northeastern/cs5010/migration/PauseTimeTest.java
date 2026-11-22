package com.northeastern.cs5010.migration;

import static org.junit.jupiter.api.Assertions.*;

import com.northeastern.cs5010.migration.LocalCluster.Node;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Migration pause time for actors with 1 KB to 1 MB of state, and the shell command that measures it. */
class PauseTimeTest {

    private LocalCluster cluster;
    private NodeShell pi;

    @BeforeEach
    void setUp() throws Exception {
        Map<String, Supplier<? extends MigratableActor>> f = new HashMap<>(LocalCluster.factories());
        f.put(BlobActor.TYPE, BlobActor::new);
        cluster = LocalCluster.of(LocalCluster.FAST, Map.of(), Map.of("pi", f, "cloud", f), "pi", "cloud");
        Node node = cluster.node("pi");
        pi = new NodeShell(node.manager, node.host, node.registry, node.transport);
    }

    @AfterEach
    void tearDown() {
        cluster.close();
    }

    @Test
    void blobActorHoldsTheRequestedState() {
        BlobActor a = new BlobActor();
        a.onMessage("fill 1000");
        a.onMessage("hello");
        assertEquals(1000, a.size());
        assertEquals(1, a.count());
        BlobActor b = new BlobActor();
        b.restore(a.snapshot());
        assertEquals(a.snapshot(), b.snapshot());
        assertEquals(1000, b.size());
        assertEquals(0, BlobActor.text(0).length());
        assertEquals(27, BlobActor.text(27).length());
        assertThrows(IllegalArgumentException.class, () -> BlobActor.text(-1));
        assertThrows(IllegalArgumentException.class, () -> b.restore("no colon"));
        assertTrue(NodeMain.defaultHost().canInstall(BlobActor.TYPE), "the Pi and cloud nodes can host blobs");
    }

    @Test
    void anActorWithOneMegabyteOfStateMigratesOverRealTcp() throws Exception {
        Node source = cluster.node("pi");
        Node destination = cluster.node("cloud");
        source.manager.spawn("big", BlobActor.TYPE);
        source.manager.send("big", "fill 1048576");
        source.manager.send("big", "one");
        MigrationManager.Result r = source.manager.migrate("big", "cloud");
        assertTrue(r.committed(), r.toString());
        BlobActor moved = destination.host.inspect("big", a -> (BlobActor) a);
        assertEquals(1_048_576, moved.size());
        assertEquals(1, moved.count());
    }

    @Test
    void pauseCommandReportsMeanAndSpreadOverRuns() {
        String out = pi.execute("pause cloud 1024 3");
        assertTrue(out.matches("pause 1024 bytes to cloud, 3 runs: mean \\d+\\.\\d\\d ms, sd \\d+\\.\\d\\d ms, "
            + "min \\d+\\.\\d\\d ms, max \\d+\\.\\d\\d ms, 3 of 3 committed"), out);
        String big = pi.execute("pause cloud 1048576 1");
        assertTrue(big.endsWith(" sd 0.00 ms, min " + big.split("min ")[1].split(" ")[0] + " ms, max "
            + big.split("max ")[1].split(" ")[0] + " ms, 1 of 1 committed"), big);
        assertTrue(cluster.node("cloud").host.hosts("pause-pi-4"), "every run's actor ends up on the destination");
    }

    @Test
    void pauseCommandReportsFailuresAndBadArguments() {
        String out = pi.execute("pause mars 10 2");
        assertTrue(out.endsWith("0 of 2 committed (ABORTED: unknown destination mars)"), out);
        assertEquals("error: usage: pause <node> <bytes> [runs]", pi.execute("pause"));
        assertEquals("error: usage: pause <node> <bytes> [runs]", pi.execute("pause cloud"));
        assertTrue(pi.execute("pause cloud -1").startsWith("error: bytes must be at least 0"));
        assertTrue(pi.execute("pause cloud 10 0").startsWith("error: bytes must be at least 0 and runs at least 1"));
        assertTrue(pi.execute("help").contains("pause <node> <bytes> [runs]"));
    }
}
