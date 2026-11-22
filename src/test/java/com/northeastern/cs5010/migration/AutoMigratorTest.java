package com.northeastern.cs5010.migration;

import static org.junit.jupiter.api.Assertions.*;

import com.northeastern.cs5010.migration.LocalCluster.Node;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** FR11: migrations start on their own when a node is overloaded, with the usual guarantees. */
class AutoMigratorTest {

    /** Freeze waits up to 5 s here, so a backed up mailbox can drain before the move. */
    private static final MigrationManager.Config PATIENT = new MigrationManager.Config(
        Duration.ofSeconds(1), 3, Duration.ofSeconds(5), Duration.ofSeconds(10));

    /** A counter that holds its first message until GATE opens, so a mailbox can be filled on purpose. */
    static volatile CountDownLatch gate = new CountDownLatch(0);

    /** A counter that sleeps 1 ms per message, so a fast sender overloads it. */
    static final class Busy implements MigratableActor {
        final CounterActor counter = new CounterActor();
        final long sleepMillis;

        Busy(long sleepMillis) {
            this.sleepMillis = sleepMillis;
        }

        @Override
        public void onMessage(String payload) {
            try {
                gate.await();
                if (sleepMillis > 0) {
                    Thread.sleep(sleepMillis);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            counter.onMessage(payload);
        }

        @Override
        public String snapshot() {
            return counter.snapshot();
        }

        @Override
        public void restore(String state) {
            counter.restore(state);
        }
    }

    private LocalCluster cluster;
    private AutoMigrator auto;

    @AfterEach
    void tearDown() {
        gate = new CountDownLatch(0);
        if (auto != null) {
            auto.close();
        }
        if (cluster != null) {
            cluster.close();
        }
    }

    private void start() throws Exception {
        Map<String, Supplier<? extends MigratableActor>> f = new HashMap<>(LocalCluster.factories());
        f.put("gated", () -> new Busy(0));
        f.put("busy", () -> new Busy(1));
        cluster = LocalCluster.of(PATIENT, Map.of(), Map.of("pi", f, "cloud", f), "pi", "cloud");
    }

    private AutoMigrator migrator(AutoMigrator.Policy policy, double cpu, AtomicLong nanos) {
        Node pi = cluster.node("pi");
        return new AutoMigrator(pi.manager, pi.host, policy, () -> cpu, nanos::get);
    }

    private static CounterActor state(Node node, String id) {
        CounterActor copy = new CounterActor();
        copy.restore(node.host.snapshot(id));
        return copy;
    }

    private static void awaitState(Node node, String id, long expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (node.host.hosts(id) && state(node, id).count() == expected) {
                return;
            }
            Thread.sleep(5);
        }
        fail(id + " on " + node.name + " never reached " + expected);
    }

    @Test
    void quietNodeDoesNotMigrate() throws Exception {
        start();
        cluster.node("pi").manager.spawn("c", CounterActor.TYPE);
        auto = migrator(new AutoMigrator.Policy("cloud", 10, 0.9, Duration.ofMillis(50), Duration.ZERO), 0.2,
            new AtomicLong());
        assertEquals(Optional.empty(), auto.check());
        assertEquals(0, auto.triggeredCount());
        assertNull(auto.lastDecision());
        assertTrue(cluster.node("pi").host.hosts("c"));
    }

    @Test
    void emptyNodeDoesNotMigrateEvenUnderCpuLoad() throws Exception {
        start();
        auto = migrator(new AutoMigrator.Policy("cloud", 10, 0.5, Duration.ofMillis(50), Duration.ZERO), 1.0,
            new AtomicLong());
        assertEquals(Optional.empty(), auto.check());
    }

    @Test
    void deepMailboxMovesTheBusiestActorWithoutLosingMessages() throws Exception {
        start();
        Node pi = cluster.node("pi");
        Node cloud = cluster.node("cloud");
        pi.manager.spawn("quiet", CounterActor.TYPE);
        pi.manager.spawn("hot", "gated");
        gate = new CountDownLatch(1);
        for (int i = 1; i <= 30; i++) {
            pi.manager.send("hot", Integer.toString(i));
        }
        auto = migrator(new AutoMigrator.Policy("cloud", 10, 0, Duration.ofMillis(50), Duration.ZERO), 0,
            new AtomicLong());
        CountDownLatch opener = gate;
        Thread release = new Thread(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            opener.countDown(); // the freeze waits for these 30 queued messages, then the actor moves
        });
        release.start();
        AutoMigrator.Decision d = auto.check().orElseThrow();
        release.join();
        assertEquals("hot", d.actorId());
        assertTrue(d.reason().startsWith("mailbox depth 30 reached 10"), d.reason());
        assertTrue(d.result().committed(), d.result().toString());
        assertTrue(cloud.host.hosts("hot"));
        assertTrue(pi.host.hosts("quiet"), "only the overloaded actor moves");
        for (int i = 31; i <= 40; i++) {
            pi.manager.send("hot", Integer.toString(i)); // forwarded to cloud
        }
        awaitState(cloud, "hot", 40);
        LocalCluster.assertInvariants(state(cloud, "hot"), 40);
        assertEquals(1, auto.triggeredCount());
        assertEquals(1, auto.committedCount());
        assertSame(d, auto.lastDecision());
    }

    @Test
    void cpuLoadTriggersEvenWithShortMailboxes() throws Exception {
        start();
        cluster.node("pi").manager.spawn("c", CounterActor.TYPE);
        auto = migrator(new AutoMigrator.Policy("cloud", 1_000, 0.9, Duration.ofMillis(50), Duration.ZERO), 0.95,
            new AtomicLong());
        AutoMigrator.Decision d = auto.check().orElseThrow();
        assertEquals("c", d.actorId());
        assertEquals("CPU load 95% reached 90%", d.reason());
        assertTrue(d.result().committed());
        assertTrue(cluster.node("cloud").host.hosts("c"));
    }

    @Test
    void cooldownSpacesOutMigrations() throws Exception {
        start();
        Node pi = cluster.node("pi");
        pi.manager.spawn("a", CounterActor.TYPE);
        pi.manager.spawn("b", CounterActor.TYPE);
        AtomicLong nanos = new AtomicLong();
        auto = migrator(new AutoMigrator.Policy("cloud", 1_000, 0.5, Duration.ofMillis(50), Duration.ofSeconds(10)),
            0.9, nanos);
        assertEquals("a", auto.check().orElseThrow().actorId(), "ties go to the first id");
        nanos.addAndGet(Duration.ofSeconds(9).toNanos());
        assertEquals(Optional.empty(), auto.check(), "still cooling down");
        nanos.addAndGet(Duration.ofSeconds(2).toNanos());
        assertEquals("b", auto.check().orElseThrow().actorId());
        assertEquals(2, auto.committedCount());
    }

    @Test
    void failedAttemptIsReportedAndAlsoCoolsDown() throws Exception {
        start();
        Node pi = cluster.node("pi");
        pi.manager.spawn("s", MisbehavingActors.NO_SNAPSHOT);
        AtomicLong nanos = new AtomicLong();
        auto = migrator(new AutoMigrator.Policy("cloud", 1_000, 0.5, Duration.ofMillis(50), Duration.ofSeconds(10)),
            0.9, nanos);
        AutoMigrator.Decision d = auto.check().orElseThrow();
        assertEquals(MigrationManager.Outcome.ABORTED, d.result().outcome());
        assertTrue(pi.host.hosts("s"), "an aborted move leaves the actor where it was");
        assertEquals(Optional.empty(), auto.check());
        assertEquals(1, auto.triggeredCount());
        assertEquals(0, auto.committedCount());
    }

    @Test
    void runsOnItsOwnUnderLoadAndKeepsEveryMessage() throws Exception {
        start();
        Node pi = cluster.node("pi");
        Node cloud = cluster.node("cloud");
        pi.manager.spawn("w", "busy");
        auto = new AutoMigrator(pi.manager, pi.host,
            new AutoMigrator.Policy("cloud", 20, 0, Duration.ofMillis(20), Duration.ofSeconds(30)));
        auto.start();
        auto.start(); // a second start has no effect
        int n = 600;
        for (int i = 1; i <= n; i++) {
            pi.manager.send("w", Integer.toString(i));
            if (i % 50 == 0) {
                Thread.sleep(5);
            }
        }
        // Wait for the whole handoff: the source drops its frozen copy only
        // after COMMITTED, a moment after the destination installed the actor.
        long deadline = System.currentTimeMillis() + 10_000;
        while (auto.committedCount() < 1 && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(1, auto.committedCount(), "the overloaded worker was moved automatically");
        assertTrue(cloud.host.hosts("w"));
        assertFalse(pi.host.hosts("w"));
        awaitState(cloud, "w", n);
        LocalCluster.assertInvariants(state(cloud, "w"), n);
        assertEquals(1, auto.triggeredCount(), "the cooldown and the move itself stop further triggers");
    }

    @Test
    void policyValidation() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> AutoMigrator.Policy.of(null, 10, 0));
        assertThrows(IllegalArgumentException.class, () -> AutoMigrator.Policy.of(" ", 10, 0));
        assertThrows(IllegalArgumentException.class, () -> AutoMigrator.Policy.of("cloud", 0, 0));
        assertThrows(IllegalArgumentException.class, () -> AutoMigrator.Policy.of("cloud", 10, 1.5));
        assertThrows(IllegalArgumentException.class, () -> AutoMigrator.Policy.of("cloud", 10, -0.1));
        assertThrows(IllegalArgumentException.class, () -> AutoMigrator.Policy.of("cloud", 10, Double.NaN));
        assertThrows(IllegalArgumentException.class,
            () -> new AutoMigrator.Policy("cloud", 10, 0, Duration.ZERO, Duration.ZERO));
        assertThrows(IllegalArgumentException.class,
            () -> new AutoMigrator.Policy("cloud", 10, 0, Duration.ofSeconds(1), Duration.ofSeconds(-1)));
        assertThrows(NullPointerException.class,
            () -> new AutoMigrator.Policy("cloud", 10, 0, null, Duration.ZERO));
        assertEquals("to cloud when a mailbox reaches 50 or CPU load reaches 80%",
            AutoMigrator.Policy.of("cloud", 50, 0.8).describe());
        assertEquals("to cloud when a mailbox reaches 50", AutoMigrator.Policy.of("cloud", 50, 0).describe());
        start();
        Node pi = cluster.node("pi");
        assertThrows(IllegalArgumentException.class,
            () -> new AutoMigrator(pi.manager, pi.host, AutoMigrator.Policy.of("pi", 10, 0)));
        double load = AutoMigrator.systemCpuLoad();
        assertTrue(load >= 0 && load <= 1, "cpu load " + load);
    }

    @Test
    void commandLineFlags() {
        NodeConfig on = NodeConfig.parse(new String[] {"--name", "pi", "--peer", "cloud=10.0.0.2:9100",
            "--auto-migrate", "cloud", "--auto-depth", "40", "--auto-cpu", "0.75"}, Map.of());
        assertTrue(on.autoMigration());
        assertEquals("cloud", on.autoMigrate().destination());
        assertEquals(40, on.autoMigrate().mailboxThreshold());
        assertEquals(0.75, on.autoMigrate().cpuThreshold(), 1e-9);

        NodeConfig defaults = NodeConfig.parse(new String[] {"--name", "pi", "--peer", "cloud=10.0.0.2:9100",
            "--auto-migrate", "cloud"}, Map.of());
        assertEquals(NodeConfig.DEFAULT_AUTO_DEPTH, defaults.autoMigrate().mailboxThreshold());
        assertEquals(0, defaults.autoMigrate().cpuThreshold(), 1e-9);
        assertFalse(NodeConfig.parse(new String[] {"--name", "pi"}, Map.of()).autoMigration());

        for (String[] bad : new String[][] {
            {"--name", "pi", "--auto-migrate", "cloud"},
            {"--name", "pi", "--auto-depth", "5"},
            {"--name", "pi", "--auto-cpu", "0.5"},
            {"--name", "pi", "--peer", "cloud=h:1", "--auto-migrate", "cloud", "--auto-depth", "x"},
            {"--name", "pi", "--peer", "cloud=h:1", "--auto-migrate", "cloud", "--auto-depth", "0"},
            {"--name", "pi", "--peer", "cloud=h:1", "--auto-migrate", "cloud", "--auto-cpu", "2"},
            {"--name", "pi", "--peer", "cloud=h:1", "--auto-migrate", "cloud", "--auto-cpu", "0"},
            {"--name", "pi", "--peer", "cloud=h:1", "--auto-migrate", "cloud", "--auto-cpu", "x"},
        }) {
            assertThrows(IllegalArgumentException.class, () -> NodeConfig.parse(bad, Map.of()), String.join(" ", bad));
        }
        assertTrue(NodeConfig.usage().contains("--auto-migrate"));
    }

    @Test
    void nodeMainStartsTheTrigger() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        int code = NodeMain.run(new String[] {"--name", "pi", "--port", "0", "--peer", "cloud=127.0.0.1:1",
                "--auto-migrate", "cloud", "--auto-depth", "25"}, Map.of(),
            new BufferedReader(new StringReader("help\nquit\n")), new PrintStream(bytes, true, StandardCharsets.UTF_8));
        String text = bytes.toString(StandardCharsets.UTF_8);
        assertEquals(0, code);
        assertTrue(text.contains("auto-migration on: to cloud when a mailbox reaches 25"), text);
    }
}
