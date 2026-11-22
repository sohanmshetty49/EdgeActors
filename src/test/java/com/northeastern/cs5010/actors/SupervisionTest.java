package com.northeastern.cs5010.actors;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The supervision tree: restart, stop, escalate, restart budgets, and cascades. */
class SupervisionTest {

    /** What every test actor did, in order, across threads. */
    static final List<String> EVENTS = new CopyOnWriteArrayList<>();

    private final AtomicLong fakeNanos = new AtomicLong();
    private ActorSystem system;
    private final ActorRef sender = new ActorRef("test", "node");

    @BeforeEach
    void setUp() {
        EVENTS.clear();
        system = new ActorSystem("node", fakeNanos::get);
    }

    @AfterEach
    void tearDown() {
        system.shutdown();
    }

    // -------------------------------------------------------------- restart

    @Test
    void restartGivesFreshStateAndKeepsQueuedMessages() throws Exception {
        ActorRef c = system.actorOf(Counter.class, "c", Supervision.restart(3, Duration.ofMinutes(1)));
        tell(c, "inc", "inc", "inc", "boom", "inc", "inc", "report");

        await(() -> EVENTS.contains("c:count=2"));
        // count restarted from 0 (fresh state), and both messages queued after
        // the failure were still processed by the new instance
        assertEquals(1, system.restartCount(c));
        assertEquals(1, system.failureCount(c));
        assertTrue(system.isAlive(c));
        assertTrue(system.lastFailure(c) instanceof IllegalStateException);
        assertEquals(List.of("c:start", "c:stop", "c:start", "c:count=2"), EVENTS);
    }

    @Test
    void restartBudgetIsASlidingWindow() throws Exception {
        ActorRef c = system.actorOf(Counter.class, "c", Supervision.restart(2, Duration.ofSeconds(10)));
        failAndWait(c, 1);
        failAndWait(c, 2);
        fakeNanos.addAndGet(Duration.ofSeconds(11).toNanos()); // both restarts fall out of the window
        failAndWait(c, 3);
        assertTrue(system.isAlive(c), "budget refilled, so the actor was restarted again");
        failAndWait(c, 4);
        tell(c, "boom"); // third restart inside one window: budget exhausted, escalates to the root
        await(() -> !system.isAlive(c));
        assertEquals(1, system.rootStopCount());
    }

    @Test
    void failingPreStartIsBoundedByTheBudget() throws Exception {
        ActorRef f = system.actorOf(FailsOnStart.class, "f", Supervision.restart(3, Duration.ofMinutes(1)));
        await(() -> !system.isAlive(f));
        assertEquals(4, EVENTS.stream().filter("f:start-failed"::equals).count(),
            "first start plus three restarts, then the budget ran out");
        assertEquals(1, system.rootStopCount());
    }

    // ----------------------------------------------------------------- stop

    @Test
    void stopDirectiveStopsTheActorAndLaterMailIsDeadLetters() throws Exception {
        ActorRef c = system.actorOf(Counter.class, "c", Supervision.stop());
        tell(c, "boom");
        await(() -> !system.isAlive(c));
        await(() -> EVENTS.contains("c:stop"));
        tell(c, "inc", "inc");
        assertEquals(2, system.deadLetterCount());
        assertEquals(0, system.restartCount(c));
    }

    @Test
    void explicitStopCascadesToTheSubtree() throws Exception {
        ActorRef parent = system.actorOf(Counter.class, "parent");
        ActorRef child = system.spawnChild(parent, Counter.class, "child", Supervision.DEFAULT);
        ActorRef grandchild = system.spawnChild(child, Counter.class, "grandchild", Supervision.DEFAULT);
        assertEquals(List.of(child), system.children(parent));
        awaitStarted("parent", "child", "grandchild");

        system.stop(parent);
        system.stop(parent); // second stop is a no-op
        assertFalse(system.isAlive(parent));
        assertFalse(system.isAlive(child));
        assertFalse(system.isAlive(grandchild));
        await(() -> EVENTS.containsAll(List.of("parent:stop", "child:stop", "grandchild:stop")));
        assertEquals(List.of(), system.children(parent));
        assertThrows(IllegalStateException.class,
            () -> system.spawnChild(parent, Counter.class, "orphan", Supervision.DEFAULT));
    }

    // ------------------------------------------------------------- escalate

    @Test
    void exhaustedChildEscalatesAndParentRestartsTheWholeSubtree() throws Exception {
        ActorRef parent = system.actorOf(Counter.class, "parent", Supervision.restart(5, Duration.ofMinutes(1)));
        ActorRef child = system.spawnChild(parent, Counter.class, "child", Supervision.restart(1, Duration.ofMinutes(1)));
        ActorRef sibling = system.spawnChild(parent, Counter.class, "sibling", Supervision.DEFAULT);
        await(() -> EVENTS.contains("sibling:start"));

        failAndWait(child, 1); // first failure: restarted by its own budget
        tell(child, "boom"); // second failure: budget of 1 exhausted, escalates to parent
        await(() -> system.restartCount(parent) == 1);
        await(() -> EVENTS.stream().filter("sibling:start"::equals).count() == 2);

        assertTrue(system.isAlive(parent));
        assertTrue(system.isAlive(child));
        assertTrue(system.isAlive(sibling), "restarted with the parent, not stopped");
        EscalatedFailure escalated = (EscalatedFailure) system.lastFailure(parent);
        assertEquals(child, escalated.child());
        assertTrue(escalated.getCause() instanceof IllegalStateException);
        assertEquals(0, system.rootStopCount());
    }

    @Test
    void escalationToAStoppingParentStopsTheWholeSubtree() throws Exception {
        ActorRef parent = system.actorOf(Counter.class, "parent", Supervision.stop());
        ActorRef child = system.spawnChild(parent, Counter.class, "child", Supervision.escalate());
        ActorRef grandchild = system.spawnChild(child, Counter.class, "grandchild", Supervision.DEFAULT);
        awaitStarted("parent", "child", "grandchild");
        tell(child, "boom");
        // The stop runs on the child's thread and walks down the tree, so the
        // parent can be marked stopped a moment before its descendants are.
        await(() -> !system.isAlive(parent) && !system.isAlive(child) && !system.isAlive(grandchild));
        await(() -> EVENTS.containsAll(List.of("parent:stop", "child:stop", "grandchild:stop")));
    }

    @Test
    void escalationCascadesThroughSeveralLevels() throws Exception {
        ActorRef root = system.actorOf(Counter.class, "root", Supervision.restart(5, Duration.ofMinutes(1)));
        ActorRef middle = system.spawnChild(root, Counter.class, "middle", Supervision.escalate());
        ActorRef leaf = system.spawnChild(middle, Counter.class, "leaf", Supervision.escalate());
        tell(leaf, "boom");
        await(() -> system.restartCount(root) == 1);
        await(() -> system.restartCount(leaf) == 1);
        assertEquals(1, system.restartCount(middle));
        assertTrue(system.lastFailure(root) instanceof EscalatedFailure);
    }

    @Test
    void actorStoppedBeforeItStartsRunsNoLifecycleHooks() throws Exception {
        ActorRef parent = system.actorOf(Counter.class, "parent");
        awaitStarted("parent");
        // Stopped in the same breath as it is created: it may never get to start,
        // and an actor that never started must not run postStop either.
        ActorRef child = system.spawnChild(parent, Counter.class, "child", Supervision.DEFAULT);
        system.stop(child);
        await(() -> !system.isAlive(child));
        Thread.sleep(100);
        long starts = EVENTS.stream().filter("child:start"::equals).count();
        long stops = EVENTS.stream().filter("child:stop"::equals).count();
        assertEquals(starts, stops, "postStop runs exactly when preStart ran");
    }

    @Test
    void topLevelEscalationIsStoppedByTheRootGuardian() throws Exception {
        ActorRef c = system.actorOf(Counter.class, "c", Supervision.escalate());
        tell(c, "boom");
        await(() -> !system.isAlive(c));
        assertEquals(1, system.rootStopCount());
    }

    @Test
    void manySimultaneousEscalationsDoNotDeadlock() throws Exception {
        ActorRef parent = system.actorOf(Counter.class, "parent", Supervision.restart(1_000, Duration.ofMinutes(1)));
        List<ActorRef> kids = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            kids.add(system.spawnChild(parent, Counter.class, "kid" + i, Supervision.escalate()));
        }
        for (ActorRef kid : kids) {
            tell(kid, "boom");
        }
        await(() -> system.failureCount(parent) == 20);
        for (ActorRef kid : kids) {
            assertTrue(system.isAlive(kid));
            tell(kid, "report");
        }
        await(() -> EVENTS.stream().filter(e -> e.endsWith(":count=0")).count() == 20);
    }

    @Test
    void messagesSentWhileAnActorStopsAreNeverSilentlyLost() throws Exception {
        // Regression test: a send racing with stop used to leave the message in a
        // dead mailbox where nobody processed or counted it.
        java.util.concurrent.atomic.AtomicLong processed = new java.util.concurrent.atomic.AtomicLong();
        PROCESSED.set(processed);
        long sent = 0;
        for (int round = 0; round < 200; round++) {
            ActorRef c = system.actorOf(Tally.class, "tally-" + round);
            Thread stopper = new Thread(() -> system.stop(c));
            stopper.start();
            for (int i = 0; i < 50; i++) {
                tell(c, "x");
                sent++;
            }
            stopper.join();
        }
        long expected = sent;
        await(() -> processed.get() + system.deadLetterCount() == expected);
        assertEquals(expected, processed.get() + system.deadLetterCount(),
            "every message is either processed or counted as a dead letter");
    }

    // ------------------------------------------------------- core behaviour

    @Test
    void moreActorsThanAFixedPoolCouldServeAllRun() throws Exception {
        List<ActorRef> refs = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            refs.add(system.actorOf(Counter.class, "a" + i));
        }
        for (ActorRef ref : refs) {
            tell(ref, "inc", "report");
        }
        await(() -> EVENTS.stream().filter(e -> e.endsWith(":count=1")).count() == 50);
    }

    @Test
    void unknownActorsAreHandledQuietly() throws Exception {
        ActorRef ghost = new ActorRef("ghost", "node");
        assertFalse(system.isAlive(ghost));
        assertEquals(List.of(), system.children(ghost));
        assertEquals(0, system.failureCount(ghost));
        assertEquals(0, system.restartCount(ghost));
        assertNull(system.lastFailure(ghost));
        assertEquals(0, system.getMailboxDepth(ghost));
        system.stop(ghost);
        tell(ghost, "hello");
        assertEquals(1, system.deadLetterCount());
        assertThrows(IllegalStateException.class,
            () -> system.spawnChild(ghost, Counter.class, "x", Supervision.DEFAULT));
    }

    @Test
    void actorsCanSpawnTheirOwnChildren() throws Exception {
        ActorRef p = system.actorOf(Spawner.class, "spawner");
        tell(p, "spawn");
        await(() -> system.children(p).size() == 1);
        assertEquals("spawned", system.children(p).get(0).getPath());
    }

    @Test
    void failingLifecycleHooksDoNotBreakTheSystem() throws Exception {
        ActorRef h = system.actorOf(BadStop.class, "bad-stop", Supervision.restart(3, Duration.ofMinutes(1)));
        tell(h, "boom");
        await(() -> system.restartCount(h) == 1);
        system.stop(h);
        assertFalse(system.isAlive(h));
    }

    @Test
    void runnerStopMethodStopsTheActor() throws Exception {
        ActorRef c = system.actorOf(Counter.class, "c");
        await(() -> EVENTS.contains("c:start"));
        ActorCell cell = new ActorCell(c, Counter.class, null, Supervision.DEFAULT,
            new RestartBudget(1, Duration.ofSeconds(1), System::nanoTime));
        ActorRunner runner = new ActorRunner(cell, system);
        runner.stop();
        runner.run(); // returns at once because the cell is already stopped
        assertTrue(cell.stopped);
    }

    // ---------------------------------------------------------- the policy

    @Test
    void supervisionValidatesItsArguments() {
        assertEquals(Supervision.Directive.RESTART, Supervision.DEFAULT.directive());
        assertEquals(3, Supervision.DEFAULT.maxRestarts());
        assertThrows(NullPointerException.class, () -> new Supervision(null, 1, Duration.ofSeconds(1)));
        assertThrows(NullPointerException.class, () -> new Supervision(Supervision.Directive.STOP, 1, null));
        assertThrows(IllegalArgumentException.class, () -> Supervision.restart(-1, Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class, () -> Supervision.restart(1, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> Supervision.restart(1, Duration.ofSeconds(-1)));
    }

    @Test
    void restartBudgetCountsInsideTheWindowOnly() {
        RestartBudget budget = new RestartBudget(2, Duration.ofSeconds(5), fakeNanos::get);
        assertTrue(budget.tryAcquire());
        assertTrue(budget.tryAcquire());
        assertFalse(budget.tryAcquire());
        assertEquals(2, budget.used());
        fakeNanos.addAndGet(Duration.ofSeconds(5).toNanos());
        assertTrue(budget.tryAcquire());
        assertEquals(1, budget.used());
        assertFalse(new RestartBudget(0, Duration.ofSeconds(1), fakeNanos::get).tryAcquire());
    }

    // -------------------------------------------------------------- helpers

    private void tell(ActorRef to, String... payloads) throws InterruptedException {
        for (String p : payloads) {
            system.send(to, new Message(sender, to, p));
        }
    }

    private static void awaitStarted(String... names) throws InterruptedException {
        for (String name : names) {
            await(() -> EVENTS.contains(name + ":start"));
        }
    }

    private void failAndWait(ActorRef ref, long expectedFailures) throws Exception {
        tell(ref, "boom");
        await(() -> system.failureCount(ref) == expectedFailures && system.restartCount(ref) == expectedFailures);
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                fail("condition not met within 5 s; events: " + EVENTS);
            }
            Thread.sleep(5);
        }
    }

    // ---------------------------------------------------------- test actors

    /** Counts "inc", reports on "report", throws on "boom", logs lifecycle events. */
    public static class Counter extends Actor {
        private int count;

        @Override
        public void preStart() {
            EVENTS.add(self.getPath() + ":start");
        }

        @Override
        public void postStop() {
            EVENTS.add(self.getPath() + ":stop");
        }

        @Override
        public void receive(Message message) {
            switch ((String) message.getPayload()) {
                case "inc" -> count++;
                case "report" -> EVENTS.add(self.getPath() + ":count=" + count);
                case "boom" -> throw new IllegalStateException("boom in " + self.getPath());
                default -> { }
            }
        }
    }

    static final java.util.concurrent.atomic.AtomicReference<java.util.concurrent.atomic.AtomicLong> PROCESSED =
        new java.util.concurrent.atomic.AtomicReference<>(new java.util.concurrent.atomic.AtomicLong());

    /** Counts every message it processes into PROCESSED. */
    public static class Tally extends Actor {
        @Override
        public void receive(Message message) {
            PROCESSED.get().incrementAndGet();
        }
    }

    /** Fails every time it starts. */
    public static class FailsOnStart extends Actor {
        @Override
        public void preStart() {
            EVENTS.add(self.getPath() + ":start-failed");
            throw new IllegalStateException("cannot start");
        }

        @Override
        public void receive(Message message) {
        }
    }

    /** Spawns a child of itself when asked. */
    public static class Spawner extends Actor {
        @Override
        public void receive(Message message) {
            try {
                system().spawnChild(self, Counter.class, "spawned", Supervision.DEFAULT);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }

    /** Throws from postStop, which the system must survive. */
    public static class BadStop extends Actor {
        @Override
        public void postStop() {
            throw new IllegalStateException("postStop failed");
        }

        @Override
        public void receive(Message message) {
            throw new IllegalStateException("boom");
        }
    }
}
