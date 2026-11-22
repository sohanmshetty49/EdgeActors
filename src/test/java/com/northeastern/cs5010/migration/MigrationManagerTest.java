package com.northeastern.cs5010.migration;

import static org.junit.jupiter.api.Assertions.*;

import com.northeastern.cs5010.migration.HandoffMessage.Type;
import com.northeastern.cs5010.migration.MigrationManager.Config;
import com.northeastern.cs5010.migration.MigrationManager.Outcome;
import com.northeastern.cs5010.migration.MigrationManager.Result;
import com.northeastern.cs5010.migration.LocalCluster.Node;
import com.northeastern.cs5010.transport.LocationRegistry.Status;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/**
 * Live migration over real TCP between nodes in one JVM. Each test checks the
 * three invariants (no loss, no duplication, single live location) for one
 * success or failure scenario.
 */
class MigrationManagerTest {

    // ------------------------------------------------------- happy paths

    @Test
    void commitMovesStateAndUpdatesBothRegistries() throws Exception {
        try (LocalCluster c = LocalCluster.of("pi", "cloud")) {
            Node pi = c.node("pi");
            Node cloud = c.node("cloud");
            pi.manager.spawn("c", CounterActor.TYPE);
            for (int i = 1; i <= 100; i++) {
                pi.manager.send("c", Integer.toString(i));
            }
            Result r = pi.manager.migrate("c", "cloud");

            assertEquals(Outcome.COMMITTED, r.outcome(), r.reason());
            assertTrue(r.committed());
            assertTrue(r.durationMillis() >= 0);
            cloud.awaitCount("c", 100);
            LocalCluster.assertInvariants(cloud.counter("c"), 100);
            assertFalse(pi.host.hosts("c"), "single live location");
            assertEquals("cloud", pi.registry.getLocation("c"));
            assertEquals(1, pi.registry.getEpoch("c"));
            assertEquals("cloud", cloud.registry.getLocation("c"));
            assertEquals(1, cloud.registry.getEpoch("c"));
            assertEquals(Status.ACTIVE, cloud.registry.getStatus("c"));
            assertEquals(1, pi.manager.commitCount());
        }
    }

    @Test
    void invariantsHoldWhileMessagesKeepArrivingDuringMigration() throws Exception {
        try (LocalCluster c = LocalCluster.of("pi", "cloud")) {
            Node pi = c.node("pi");
            Node cloud = c.node("cloud");
            pi.manager.spawn("c", CounterActor.TYPE);
            AtomicReference<Throwable> error = new AtomicReference<>();
            Thread sender = sender(pi.manager, "c", 1, 3_000, error);
            Thread.sleep(5);
            Result r = pi.manager.migrate("c", "cloud");
            sender.join();

            assertNull(error.get());
            assertEquals(Outcome.COMMITTED, r.outcome(), r.reason());
            cloud.awaitCount("c", 3_000);
            LocalCluster.assertInvariants(cloud.counter("c"), 3_000);
            assertFalse(pi.host.hosts("c"));
        }
    }

    @Test
    void messagesSentToOldLocationAreForwarded() throws Exception {
        try (LocalCluster c = LocalCluster.of("pi", "cloud")) {
            Node pi = c.node("pi");
            Node cloud = c.node("cloud");
            pi.manager.spawn("c", CounterActor.TYPE);
            assertTrue(pi.manager.migrate("c", "cloud").committed());
            for (int i = 1; i <= 50; i++) {
                pi.manager.send("c", Integer.toString(i));
            }
            cloud.awaitCount("c", 50);
            LocalCluster.assertInvariants(cloud.counter("c"), 50);
            assertEquals(50, pi.manager.forwardedCount());
        }
    }

    @Test
    void forwardingFollowsAChainOfMoves() throws Exception {
        try (LocalCluster c = LocalCluster.of("pi", "edge2", "cloud")) {
            Node pi = c.node("pi");
            Node edge2 = c.node("edge2");
            Node cloud = c.node("cloud");
            pi.manager.spawn("c", CounterActor.TYPE);
            assertTrue(pi.manager.migrate("c", "edge2").committed());
            assertTrue(edge2.manager.migrate("c", "cloud").committed());
            for (int i = 1; i <= 100; i++) {
                pi.manager.send("c", Integer.toString(i)); // pi still thinks c is on edge2
            }
            cloud.awaitCount("c", 100);
            LocalCluster.assertInvariants(cloud.counter("c"), 100);
            assertEquals(2, cloud.registry.getEpoch("c"));
            // edge2 counts a forward just after sending it, so cloud can finish
            // the last message a moment before edge2's counter catches up.
            awaitForwarded(edge2, 100);
        }
    }

    // -------------------------------------------------- rejected handoffs

    @Test
    void destinationWithoutFactoryRejectsAndActorStaysPut() throws Exception {
        Map<String, Supplier<? extends MigratableActor>> slowOnly = Map.of(MisbehavingActors.SLOW, MisbehavingActors.Slow::new);
        try (LocalCluster c = LocalCluster.of(LocalCluster.FAST, Map.of(), Map.of("cloud", slowOnly), "pi", "cloud")) {
            Node pi = c.node("pi");
            pi.manager.spawn("c", CounterActor.TYPE);
            Result r = pi.manager.migrate("c", "cloud");

            assertEquals(Outcome.ABORTED, r.outcome());
            assertTrue(r.reason().contains("no factory"), r.reason());
            assertActiveAt(pi, "c", 0);
            pi.manager.send("c", "1");
            pi.awaitCount("c", 1);
            assertEquals(1, pi.manager.abortCount());
        }
    }

    @Test
    void destinationAlreadyHostingRejects() throws Exception {
        try (LocalCluster c = LocalCluster.of("pi", "cloud")) {
            c.node("cloud").manager.spawn("c", CounterActor.TYPE);
            c.node("pi").manager.spawn("c", CounterActor.TYPE);
            Result r = c.node("pi").manager.migrate("c", "cloud");
            assertEquals(Outcome.ABORTED, r.outcome());
            assertTrue(r.reason().contains("already hosted"), r.reason());
        }
    }

    @Test
    void installFailureOnDestinationRejectsCommit() throws Exception {
        try (LocalCluster c = LocalCluster.of("pi", "cloud")) {
            Node pi = c.node("pi");
            pi.manager.spawn("x", MisbehavingActors.NO_RESTORE);
            Result r = pi.manager.migrate("x", "cloud");
            assertEquals(Outcome.ABORTED, r.outcome());
            assertTrue(r.reason().contains("install failed"), r.reason());
            assertActiveAt(pi, "x", 0);
            assertFalse(c.node("cloud").host.hosts("x"));
        }
    }

    @Test
    void snapshotFailureAbortsBeforeContactingDestination() throws Exception {
        try (LocalCluster c = LocalCluster.of("pi", "cloud")) {
            Node pi = c.node("pi");
            pi.manager.spawn("x", MisbehavingActors.NO_SNAPSHOT);
            Result r = pi.manager.migrate("x", "cloud");
            assertEquals(Outcome.ABORTED, r.outcome());
            assertTrue(r.reason().contains("snapshot failed"), r.reason());
            assertActiveAt(pi, "x", 0);
            assertEquals(0, c.node("cloud").transport.framesReceived());
        }
    }

    @Test
    void busyActorAbortsInsteadOfMigratingHalfDone() throws Exception {
        try (LocalCluster c = LocalCluster.of("pi", "cloud")) {
            Node pi = c.node("pi");
            pi.manager.spawn("s", MisbehavingActors.SLOW);
            for (int i = 0; i < 5; i++) {
                pi.manager.send("s", "work"); // 5 x 200 ms, freeze timeout is 500 ms
            }
            Result r = pi.manager.migrate("s", "cloud");
            assertEquals(Outcome.ABORTED, r.outcome());
            assertTrue(r.reason().contains("did not become idle"), r.reason());
            assertActiveAt(pi, "s", 0);
            assertTrue(pi.host.awaitIdle("s", Duration.ofSeconds(3)));
            assertEquals(5L, (long) pi.host.inspect("s", a -> ((MisbehavingActors.Slow) a).handled));
        }
    }

    @Test
    void invalidRequestsAreRefused() throws Exception {
        try (LocalCluster c = LocalCluster.of("pi", "cloud")) {
            Node pi = c.node("pi");
            pi.manager.spawn("c", CounterActor.TYPE);
            pi.manager.learn("remote", "cloud", 0);
            assertEquals(Outcome.ABORTED, pi.manager.migrate("c", "pi").outcome());
            assertEquals(Outcome.ABORTED, pi.manager.migrate("c", "mars").outcome());
            assertEquals(Outcome.ABORTED, pi.manager.migrate("nobody", "cloud").outcome());
            assertEquals(Outcome.ABORTED, pi.manager.migrate("remote", "cloud").outcome());
            Result noDoubt = pi.manager.resolve("c");
            assertEquals(Outcome.ABORTED, noDoubt.outcome());
            assertTrue(noDoubt.reason().contains("no in-doubt"));
            assertThrows(IllegalArgumentException.class, () -> pi.manager.send("nobody", "x"));
            assertThrows(IllegalArgumentException.class, () -> pi.manager.learn("y", "mars", 0));
            assertThrows(IllegalStateException.class, () -> pi.manager.spawn("c", CounterActor.TYPE));
            assertThrows(IllegalArgumentException.class, () -> pi.manager.spawn("z", "no-such-type"));
            assertEquals("pi", pi.manager.nodeName());
        }
    }

    @Test
    void onlyOneMigrationOfAnActorRunsAtATime() throws Exception {
        try (LocalCluster c = LocalCluster.of("pi", "cloud")) {
            Node pi = c.node("pi");
            pi.manager.spawn("c", CounterActor.TYPE);
            c.node("cloud").faulty.dropAll(Type.PREPARED); // keeps the first migration waiting
            CompletableFuture<Result> first = CompletableFuture.supplyAsync(() -> pi.manager.migrate("c", "cloud"));
            while (pi.registry.getStatus("c") != Status.MIGRATING) {
                Thread.sleep(1);
            }
            Result second = pi.manager.migrate("c", "cloud");
            assertEquals(Outcome.ABORTED, second.outcome());
            assertTrue(second.reason().contains("not active"), second.reason());
            assertEquals(Outcome.ABORTED, first.get().outcome());
        }
    }

    // ---------------------------------------------- lost and slow messages

    @Test
    void unreachableDestinationAbortsAndKeepsActor() throws Exception {
        try (LocalCluster c = LocalCluster.of("pi", "cloud")) {
            Node pi = c.node("pi");
            pi.manager.spawn("c", CounterActor.TYPE);
            c.node("cloud").transport.close();
            Result r = pi.manager.migrate("c", "cloud");
            assertEquals(Outcome.ABORTED, r.outcome());
            assertTrue(r.reason().contains("no reply to PREPARE"), r.reason());
            assertActiveAt(pi, "c", 0);
        }
    }

    @Test
    void lostPreparedReplyAbortsAndReplaysBufferedMessagesLocally() throws Exception {
        try (LocalCluster c = LocalCluster.of("pi", "cloud")) {
            Node pi = c.node("pi");
            Node cloud = c.node("cloud");
            pi.manager.spawn("c", CounterActor.TYPE);
            cloud.faulty.dropAll(Type.PREPARED);
            AtomicReference<Throwable> error = new AtomicReference<>();
            Thread sender = sender(pi.manager, "c", 1, 1_000, error);
            Result r = pi.manager.migrate("c", "cloud");
            sender.join();

            assertNull(error.get());
            assertEquals(Outcome.ABORTED, r.outcome());
            assertTrue(r.reason().contains("no reply to PREPARE"), r.reason());
            pi.awaitCount("c", 1_000);
            LocalCluster.assertInvariants(pi.counter("c"), 1_000);
            assertActiveAt(pi, "c", 0);
            assertFalse(cloud.host.hosts("c"));

            // ABORT reached the destination and cleared its held state, so a retry succeeds.
            cloud.faulty.heal();
            assertTrue(pi.manager.migrate("c", "cloud").committed());
            cloud.awaitCount("c", 1_000);
        }
    }

    @Test
    void lostCommittedReplyParksActorThenResolveFinishesTheMove() throws Exception {
        try (LocalCluster c = LocalCluster.of("pi", "cloud")) {
            Node pi = c.node("pi");
            Node cloud = c.node("cloud");
            pi.manager.spawn("c", CounterActor.TYPE);
            for (int i = 1; i <= 500; i++) {
                pi.manager.send("c", Integer.toString(i));
            }
            cloud.faulty.dropAll(Type.COMMITTED);
            Result r = pi.manager.migrate("c", "cloud");

            assertEquals(Outcome.IN_DOUBT, r.outcome(), r.reason());
            assertTrue(pi.manager.isInDoubt("c"));
            assertEquals(Status.PAUSED, pi.registry.getStatus("c"));
            // The destination applied COMMIT once even though it arrived three times.
            cloud.awaitCount("c", 500);
            assertEquals(3, cloud.faulty.dropped());

            for (int i = 501; i <= 600; i++) {
                pi.manager.send("c", Integer.toString(i)); // held at pi while in doubt
            }
            cloud.faulty.heal();
            Result resolved = pi.manager.resolve("c");

            assertEquals(Outcome.COMMITTED, resolved.outcome(), resolved.reason());
            assertFalse(pi.manager.isInDoubt("c"));
            cloud.awaitCount("c", 600);
            LocalCluster.assertInvariants(cloud.counter("c"), 600);
            assertFalse(pi.host.hosts("c"));
            assertEquals("cloud", pi.registry.getLocation("c"));
        }
    }

    @Test
    void lostCommitParksActorThenResolveRestoresItAtSource() throws Exception {
        try (LocalCluster c = LocalCluster.of("pi", "cloud")) {
            Node pi = c.node("pi");
            Node cloud = c.node("cloud");
            pi.manager.spawn("c", CounterActor.TYPE);
            pi.faulty.dropAll(Type.COMMIT);
            AtomicReference<Throwable> error = new AtomicReference<>();
            Thread sender = sender(pi.manager, "c", 1, 1_000, error);
            Result r = pi.manager.migrate("c", "cloud");
            sender.join();
            assertNull(error.get());
            assertEquals(Outcome.IN_DOUBT, r.outcome(), r.reason());

            pi.faulty.dropAll(Type.QUERY);
            Result stillDown = pi.manager.resolve("c");
            assertEquals(Outcome.IN_DOUBT, stillDown.outcome());
            assertTrue(stillDown.reason().contains("still not answering"));

            pi.faulty.heal();
            Result resolved = pi.manager.resolve("c");
            assertEquals(Outcome.ABORTED, resolved.outcome());
            assertTrue(resolved.reason().contains("never committed"), resolved.reason());
            pi.awaitCount("c", 1_000);
            LocalCluster.assertInvariants(pi.counter("c"), 1_000);
            assertActiveAt(pi, "c", 0);
            assertFalse(cloud.host.hosts("c"));
        }
    }

    @Test
    void commitArrivingAfterLeaseExpiryIsRejected() throws Exception {
        Config shortLease = new Config(Duration.ofMillis(300), 3, Duration.ofMillis(500), Duration.ofMillis(50));
        try (LocalCluster c = LocalCluster.of(LocalCluster.FAST, Map.of("cloud", shortLease), "pi", "cloud")) {
            Node pi = c.node("pi");
            pi.manager.spawn("c", CounterActor.TYPE);
            pi.manager.send("c", "1");
            pi.faulty.dropFirst(Type.COMMIT, 1); // the retry arrives after the 50 ms lease
            Result r = pi.manager.migrate("c", "cloud");
            assertEquals(Outcome.ABORTED, r.outcome());
            assertTrue(r.reason().contains("no prepared state"), r.reason());
            assertActiveAt(pi, "c", 0);
            pi.awaitCount("c", 1);
            assertFalse(c.node("cloud").host.hosts("c"));
        }
    }

    // --------------------------------------------------------- forwarding

    @Test
    void staleRouteIsDroppedInsteadOfBouncingForever() throws Exception {
        try (LocalCluster c = LocalCluster.of("a", "b")) {
            Node a = c.node("a");
            Node b = c.node("b");
            a.manager.learn("ghost", "b", 5);
            b.manager.learn("ghost", "a", 3); // b's information is older than a's
            a.manager.send("ghost", "1");
            awaitDropped(b, 1);
            assertEquals(1, a.manager.forwardedCount());
            assertEquals(0, b.manager.forwardedCount());
        }
    }

    @Test
    void forwardForUnknownActorIsDropped() throws Exception {
        try (LocalCluster c = LocalCluster.of("a", "b")) {
            c.node("a").manager.learn("y", "b", 0);
            c.node("a").manager.send("y", "hello");
            awaitDropped(c.node("b"), 1);
        }
    }

    @Test
    void forwardingToADeadNodeReportsTheError() throws Exception {
        try (LocalCluster c = LocalCluster.of("a", "b")) {
            c.node("a").manager.learn("y", "b", 0);
            c.node("b").transport.close();
            assertThrows(IOException.class, () -> c.node("a").manager.send("y", "hello"));
        }
    }

    @Test
    void malformedProtocolMessageIsIgnored() throws Exception {
        try (LocalCluster c = LocalCluster.of("a", "b")) {
            Node a = c.node("a");
            Node b = c.node("b");
            a.transport.send("127.0.0.1:" + b.transport.boundPort(), "%%% not a handoff message %%%");
            awaitDropped(b, 1);
            a.manager.spawn("c", CounterActor.TYPE);
            assertTrue(a.manager.migrate("c", "b").committed());
        }
    }

    @Test
    void duplicatePrepareForTheSameActorIsRefused() throws Exception {
        try (LocalCluster c = LocalCluster.of("pi", "cloud")) {
            Node pi = c.node("pi");
            String cloudAddress = "127.0.0.1:" + c.node("cloud").transport.boundPort();
            // A stray PREPARE for "c" makes cloud hold state for it under a lease. Its
            // PREPARED reply reaches pi too, but carries no matching requestId, so the
            // real migration below cannot mistake it for its own answer.
            pi.transport.send(cloudAddress, HandoffMessage.prepare("c", CounterActor.TYPE, 0, "0,0,0,0").encode());
            pi.manager.spawn("c", CounterActor.TYPE);
            Result r = pi.manager.migrate("c", "cloud");
            assertEquals(Outcome.ABORTED, r.outcome());
            assertFalse(r.committed());
            assertTrue(r.reason().contains("another handoff"), r.reason());
            assertActiveAt(pi, "c", 0);
        }
    }

    @Test
    void messagesFromUnknownNodesAreNotAnsweredAndDoNoHarm() throws Exception {
        try (LocalCluster c = LocalCluster.of("pi", "cloud")) {
            String cloudAddress = "127.0.0.1:" + c.node("cloud").transport.boundPort();
            com.northeastern.cs5010.transport.TCPTransport stranger =
                new com.northeastern.cs5010.transport.TCPTransport("stranger", 0);
            stranger.start();
            try {
                stranger.send(cloudAddress, HandoffMessage.prepare("x", CounterActor.TYPE, 0, "0,0,0,0").encode());
                stranger.send(cloudAddress, new HandoffMessage(Type.FORWARD, "x", null, 0, 0, null, null, null).encode());
                awaitDropped(c.node("cloud"), 1); // the empty FORWARD has no route
                assertNull(stranger.receive("cloud", 300), "no reply goes to an unknown node");
            } finally {
                stranger.close();
            }
            c.node("pi").manager.spawn("c", CounterActor.TYPE);
            assertTrue(c.node("pi").manager.migrate("c", "cloud").committed());
        }
    }

    @Test
    void learnCanRecordTheLocalNode() throws Exception {
        try (LocalCluster c = LocalCluster.of("pi", "cloud")) {
            c.node("pi").manager.learn("k", "pi", 4);
            assertEquals("pi", c.node("pi").registry.getLocation("k"));
            assertEquals(4, c.node("pi").registry.getEpoch("k"));
        }
    }

    // ------------------------------------------------------------- config

    @Test
    void configValidation() {
        assertThrows(IllegalArgumentException.class,
            () -> new Config(Duration.ofSeconds(1), 0, Duration.ofSeconds(1), Duration.ofSeconds(1)));
        assertThrows(NullPointerException.class,
            () -> new Config(null, 1, Duration.ofSeconds(1), Duration.ofSeconds(1)));
        Config d = Config.defaults();
        assertEquals(3, d.commitAttempts());
        assertTrue(d.prepareLease().compareTo(d.replyTimeout().multipliedBy(d.commitAttempts())) > 0,
            "the lease must outlive every COMMIT retry");
    }

    // ------------------------------------------------------------ helpers

    private static Thread sender(MigrationManager via, String actorId, long from, long to,
                                 AtomicReference<Throwable> error) {
        Thread t = new Thread(() -> {
            try {
                for (long i = from; i <= to; i++) {
                    via.send(actorId, Long.toString(i));
                    if (i % 50 == 0) {
                        Thread.sleep(1); // spread sends out so the migration overlaps them
                    }
                }
            } catch (Throwable e) {
                error.set(e);
            }
        });
        t.start();
        return t;
    }

    private static void assertActiveAt(Node node, String actorId, long epoch) {
        assertTrue(node.host.hosts(actorId));
        assertEquals(node.name, node.registry.getLocation(actorId));
        assertEquals(epoch, node.registry.getEpoch(actorId));
        assertEquals(Status.ACTIVE, node.registry.getStatus(actorId));
        assertFalse(node.manager.isInDoubt(actorId));
    }

    private static void awaitForwarded(Node node, long expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (node.manager.forwardedCount() < expected && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(expected, node.manager.forwardedCount());
    }

    private static void awaitDropped(Node node, long expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (node.manager.droppedCount() < expected && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(expected, node.manager.droppedCount());
    }
}
