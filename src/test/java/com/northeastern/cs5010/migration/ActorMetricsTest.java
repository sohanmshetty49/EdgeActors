package com.northeastern.cs5010.migration;

import static org.junit.jupiter.api.Assertions.*;

import com.northeastern.cs5010.migration.LocalCluster.Node;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Per-actor metrics on a node: mailbox depth, processed, processing time, current node (FR12). */
class ActorMetricsTest {

    private LocalCluster cluster;

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    void hostReportsProcessedCountTimeAndFailures() throws Exception {
        LocalActorHost host = new LocalActorHost(Map.of(
            "boom", LocalActorHostTest.ExplodingActor::new, MisbehavingActors.SLOW, MisbehavingActors.Slow::new));
        host.install("b", "boom", null);
        host.deliver("b", "ok");
        host.deliver("b", "explode");
        assertTrue(host.awaitIdle("b", Duration.ofSeconds(3)));
        LocalActorHost.ActorMetrics m = host.metrics("b");
        assertEquals(0, m.mailboxDepth());
        assertEquals(2, m.processed());
        assertEquals(1, m.failures());

        host.install("s", MisbehavingActors.SLOW, null);
        host.deliver("s", "work");
        host.deliver("s", "work");
        host.deliver("s", "work");
        assertTrue(host.mailboxDepths().get("s") >= 2, host.mailboxDepths().toString());
        assertEquals(0, (int) host.mailboxDepths().get("b"));
        assertTrue(host.awaitIdle("s", Duration.ofSeconds(5)));
        assertTrue(host.metrics("s").averageProcessingMicros() >= 200_000, "Slow sleeps 200 ms per message");
        assertThrows(IllegalArgumentException.class, () -> host.metrics("missing"));
        host.stop("b");
        host.stop("s");
    }

    @Test
    void shellShowsMetricsWhereTheActorRunsAndPointsElsewhereOtherwise() throws Exception {
        cluster = LocalCluster.of("pi", "cloud");
        Node piNode = cluster.node("pi");
        NodeShell pi = new NodeShell(piNode.manager, piNode.host, piNode.registry, piNode.transport);
        pi.execute("spawn c");
        pi.execute("send c 10");
        piNode.awaitCount("c", 10);
        String line = pi.execute("metrics c");
        assertTrue(line.startsWith("c: node=pi mailbox=0 processed=10 avgProcessingMicros="), line);
        assertTrue(line.endsWith(" failures=0 restarts=0"), line);

        assertTrue(pi.execute("migrate c cloud").startsWith("COMMITTED"));
        assertEquals("c: node=cloud (metrics are kept on that node)", pi.execute("metrics c"));
        assertEquals("nobody is unknown on pi", pi.execute("metrics nobody"));
        assertEquals("error: usage: metrics <actor>", pi.execute("metrics"));
        assertTrue(pi.execute("help").contains("metrics <actor>"));
    }
}
