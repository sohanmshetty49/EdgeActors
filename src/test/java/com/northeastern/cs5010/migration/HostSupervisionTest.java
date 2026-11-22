package com.northeastern.cs5010.migration;

import static org.junit.jupiter.api.Assertions.*;

import com.northeastern.cs5010.actors.Supervision;
import com.northeastern.cs5010.migration.LocalCluster.Node;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/** Migrated actors are supervised by their host: restart, stop, budget, dead letters. */
class HostSupervisionTest {

    private static final Map<String, Supplier<? extends MigratableActor>> BOOM =
        Map.of("boom", LocalActorHostTest.ExplodingActor::new);

    @Test
    void restartReplacesTheInstanceAndKeepsLaterMessages() throws Exception {
        LocalActorHost host = new LocalActorHost(BOOM, Supervision.restart(5, Duration.ofMinutes(1)));
        host.install("b", "boom", "7");
        host.deliver("b", "ok");
        host.deliver("b", "explode");
        host.deliver("b", "ok");
        host.deliver("b", "ok");
        assertTrue(host.awaitIdle("b", Duration.ofSeconds(3)));
        assertEquals("2", host.snapshot("b"), "fresh instance after the restart, then two more messages");
        LocalActorHost.ActorMetrics m = host.metrics("b");
        assertEquals(4, m.processed());
        assertEquals(1, m.failures());
        assertEquals(1, m.restarts());
        assertEquals(0, host.deadLetterCount());
        assertNull(host.stoppedReason("b"));
        host.stop("b");
    }

    @Test
    void exhaustedBudgetStopsTheActorAndCountsDeadLetters() throws Exception {
        LocalActorHost host = new LocalActorHost(BOOM, Supervision.restart(2, Duration.ofMinutes(1)));
        host.install("b", "boom", null);
        for (int i = 0; i < 3; i++) {
            host.deliver("b", "explode");
        }
        for (int i = 0; i < 5; i++) {
            host.deliver("b", "ok");
        }
        awaitStopped(host, "b");
        assertTrue(host.stoppedReason("b").contains("escalated"), host.stoppedReason("b"));
        assertEquals(5, host.deadLetterCount(), "the 5 messages behind the third failure");
        host.deliver("b", "ok"); // later messages are dead letters, not errors
        assertEquals(6, host.deadLetterCount());
        assertThrows(IllegalArgumentException.class, () -> host.metrics("b"));

        host.install("b", "boom", null); // the id can be used again
        assertTrue(host.hosts("b"));
        assertNull(host.stoppedReason("b"));
        host.stop("b");
    }

    @Test
    void stopPolicyStopsOnTheFirstFailure() throws Exception {
        LocalActorHost host = new LocalActorHost(BOOM, Supervision.stop());
        host.install("b", "boom", null);
        host.deliver("b", "explode");
        host.deliver("b", "ok");
        host.deliver("b", "ok");
        awaitStopped(host, "b");
        assertTrue(host.stoppedReason("b").startsWith("STOP policy"), host.stoppedReason("b"));
        assertEquals(2, host.deadLetterCount());
    }

    @Test
    void escalatePolicyStopsTheActorAtTheNodeRoot() throws Exception {
        LocalActorHost host = new LocalActorHost(BOOM, Supervision.escalate());
        host.install("b", "boom", null);
        host.deliver("b", "explode");
        awaitStopped(host, "b");
        assertEquals(0, host.deadLetterCount());
    }

    @Test
    void restartBudgetIsASlidingWindow() throws Exception {
        AtomicLong nanos = new AtomicLong();
        LocalActorHost host = new LocalActorHost(BOOM, Supervision.restart(1, Duration.ofSeconds(10)), nanos::get);
        host.install("b", "boom", null);
        host.deliver("b", "explode");
        assertTrue(host.awaitIdle("b", Duration.ofSeconds(3)));
        nanos.addAndGet(Duration.ofSeconds(11).toNanos()); // the first restart leaves the window
        host.deliver("b", "explode");
        assertTrue(host.awaitIdle("b", Duration.ofSeconds(3)));
        assertTrue(host.hosts("b"));
        assertEquals(2, host.metrics("b").restarts());
        host.deliver("b", "explode"); // second restart inside the new window: budget used up
        awaitStopped(host, "b");
    }

    @Test
    void aQueryWaitingBehindAFatalFailureFailsInsteadOfHanging() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Map<String, Supplier<? extends MigratableActor>> f = Map.of("gate", () -> new MigratableActor() {
            @Override
            public void onMessage(String payload) {
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                throw new IllegalStateException("fails after the gate opens");
            }

            @Override
            public String snapshot() {
                return "";
            }

            @Override
            public void restore(String state) {
            }
        });
        LocalActorHost host = new LocalActorHost(f, Supervision.stop());
        host.install("g", "gate", null);
        host.deliver("g", "go");
        assertTrue(entered.await(3, TimeUnit.SECONDS));
        AtomicReference<Throwable> seen = new AtomicReference<>();
        Thread asker = new Thread(() -> {
            try {
                host.snapshot("g");
            } catch (Throwable t) {
                seen.set(t);
            }
        });
        asker.start();
        Thread.sleep(50); // let the snapshot queue up behind the running message
        release.countDown();
        asker.join(3_000);
        assertFalse(asker.isAlive(), "snapshot must not hang once the actor stops");
        assertTrue(seen.get() instanceof IllegalStateException, String.valueOf(seen.get()));
    }

    @Test
    void aMigratedActorIsSupervisedOnItsNewNode() throws Exception {
        Map<String, Supplier<? extends MigratableActor>> f = new HashMap<>(LocalCluster.factories());
        f.put("boom", LocalActorHostTest.ExplodingActor::new);
        try (LocalCluster cluster = LocalCluster.of(LocalCluster.FAST, Map.of(), Map.of("pi", f, "cloud", f),
            "pi", "cloud")) {
            Node pi = cluster.node("pi");
            Node cloud = cluster.node("cloud");
            pi.manager.spawn("b", "boom");
            pi.manager.send("b", "ok");
            assertTrue(pi.manager.migrate("b", "cloud").committed());
            pi.manager.send("b", "explode"); // forwarded to cloud
            pi.manager.send("b", "ok");
            long deadline = System.currentTimeMillis() + 5_000;
            while ((!cloud.host.hosts("b") || cloud.host.metrics("b").processed() < 2)
                && System.currentTimeMillis() < deadline) {
                Thread.sleep(5);
            }
            assertEquals(1, cloud.host.metrics("b").restarts(), "cloud's host restarted the migrated actor");
            assertEquals("1", cloud.host.snapshot("b"));

            NodeShell shell = new NodeShell(cloud.manager, cloud.host, cloud.registry, cloud.transport);
            assertTrue(shell.execute("metrics b").endsWith(" failures=1 restarts=1"), shell.execute("metrics b"));
            assertTrue(shell.execute("stats").endsWith(" deadLetters=0"), shell.execute("stats"));
        }
    }

    @Test
    void shellShowsWhyAnActorWasStopped() throws Exception {
        Map<String, Supplier<? extends MigratableActor>> f = new HashMap<>(LocalCluster.factories());
        f.put("boom", LocalActorHostTest.ExplodingActor::new);
        LocalActorHost host = new LocalActorHost(f, Supervision.stop());
        com.northeastern.cs5010.transport.LocationRegistry registry = new com.northeastern.cs5010.transport.LocationRegistry();
        com.northeastern.cs5010.transport.TCPTransport transport = new com.northeastern.cs5010.transport.TCPTransport("solo", 0);
        try (MigrationManager manager = new MigrationManager(host, registry, transport, Map.of(), LocalCluster.FAST)) {
            NodeShell shell = new NodeShell(manager, host, registry, transport);
            shell.execute("spawn b boom");
            shell.execute("tell b explode");
            awaitStopped(host, "b");
            assertTrue(shell.execute("metrics b").startsWith("b: node=solo stopped by its supervisor (STOP policy"),
                shell.execute("metrics b"));
            assertEquals("sent to b", shell.execute("tell b ok"));
            assertTrue(shell.execute("stats").endsWith(" deadLetters=1"), shell.execute("stats"));
        } finally {
            transport.close();
        }
    }

    @Test
    void defaultPolicyIsTheActorSystemDefault() {
        assertEquals(Supervision.DEFAULT, new LocalActorHost(BOOM).supervision());
        assertThrows(NullPointerException.class, () -> new LocalActorHost(BOOM, null));
    }

    private static void awaitStopped(LocalActorHost host, String id) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (host.hosts(id) && System.currentTimeMillis() < deadline) {
            Thread.sleep(2);
        }
        assertFalse(host.hosts(id), id + " should have been stopped");
        assertNotNull(host.stoppedReason(id));
    }
}
