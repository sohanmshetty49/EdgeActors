package com.northeastern.cs5010.migration;

import com.northeastern.cs5010.actors.RestartBudget;
import com.northeastern.cs5010.actors.Supervision;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Runs migratable actors on this node. Each actor gets a single-threaded
 * mailbox backed by a virtual thread, so messages to one actor are processed
 * one at a time and in arrival order, and thousands of idle actors cost almost
 * nothing on a Raspberry Pi.
 *
 * <p>Actors here are supervised with the same {@link Supervision} policies as
 * the {@code ActorSystem}. When onMessage throws, the host acts as the actor's
 * supervisor on this node:
 * <ul>
 *   <li><b>RESTART</b>: a fresh instance replaces the failed one. The mailbox
 *       is kept, so only the failing message is lost. Each restart uses the
 *       actor's restart budget.</li>
 *   <li><b>STOP</b>, or <b>ESCALATE</b>, or a RESTART with the budget used up:
 *       the actor is stopped. The host is the top of the tree for migrated
 *       actors, so an escalated failure stops the actor, as the root guardian
 *       does in the ActorSystem. Messages still queued, and any sent later,
 *       are counted as dead letters, never silently lost.</li>
 * </ul>
 * The supervision policy and a fresh restart budget apply on whichever node
 * the actor is currently hosted.
 */
public final class LocalActorHost implements ActorHost {

    private static final System.Logger LOG = System.getLogger(LocalActorHost.class.getName());

    private final Map<String, Supplier<? extends MigratableActor>> factories;
    private final Supervision supervision;
    private final LongSupplier clock;
    private final Map<String, Hosted> actors = new ConcurrentHashMap<>();
    private final Map<String, String> stoppedBySupervisor = new ConcurrentHashMap<>();
    private final AtomicLong deadLetters = new AtomicLong();

    /**
     * A snapshot of one actor's metrics on this node.
     *
     * @param mailboxDepth messages queued or running right now
     * @param processed messages handled since the actor was installed here
     * @param averageProcessingMicros mean time spent in onMessage per message
     * @param failures messages whose onMessage threw
     * @param restarts times the supervisor replaced the instance after a failure
     */
    public record ActorMetrics(int mailboxDepth, long processed, double averageProcessingMicros, long failures,
                               long restarts) {
    }

    private static final class Hosted {
        final String type;
        /** Replaced only on the actor's own mailbox thread, on restart. */
        volatile MigratableActor actor;
        final RestartBudget budget;
        final ExecutorService mailbox = Executors.newSingleThreadExecutor(Thread.ofVirtual().factory());
        final AtomicInteger pending = new AtomicInteger();
        final AtomicLong processed = new AtomicLong();
        final AtomicLong processingNanos = new AtomicLong();
        final AtomicLong failures = new AtomicLong();
        final AtomicLong restarts = new AtomicLong();

        Hosted(String type, MigratableActor actor, RestartBudget budget) {
            this.type = type;
            this.actor = actor;
            this.budget = budget;
        }
    }

    /** A delivered message, so a stop can tell queued messages apart from queued queries. */
    private record Delivery(Runnable work) implements Runnable {
        @Override
        public void run() {
            work.run();
        }
    }

    /** factories maps a type name such as "counter" to a constructor; failures use {@link Supervision#DEFAULT}. */
    public LocalActorHost(Map<String, Supplier<? extends MigratableActor>> factories) {
        this(factories, Supervision.DEFAULT);
    }

    /** factories maps a type name to a constructor; supervision decides what happens when an actor fails. */
    public LocalActorHost(Map<String, Supplier<? extends MigratableActor>> factories, Supervision supervision) {
        this(factories, supervision, System::nanoTime);
    }

    /** clock returns nanoseconds and drives restart budgets; tests pass a fake clock. */
    LocalActorHost(Map<String, Supplier<? extends MigratableActor>> factories, Supervision supervision,
                   LongSupplier clock) {
        this.factories = Map.copyOf(factories);
        this.supervision = Objects.requireNonNull(supervision, "supervision");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public Supervision supervision() {
        return supervision;
    }

    @Override
    public boolean hosts(String actorId) {
        return actors.containsKey(actorId);
    }

    @Override
    public String typeOf(String actorId) {
        Hosted h = actors.get(actorId);
        return h == null ? null : h.type;
    }

    @Override
    public boolean canInstall(String actorType) {
        return factories.containsKey(actorType);
    }

    @Override
    public void install(String actorId, String actorType, String state) {
        Supplier<? extends MigratableActor> factory = factories.get(actorType);
        if (factory == null) {
            throw new IllegalArgumentException("no factory for actor type " + actorType);
        }
        MigratableActor actor = factory.get();
        if (state != null) {
            actor.restore(state);
        }
        RestartBudget budget = new RestartBudget(supervision.maxRestarts(), supervision.window(), clock);
        Hosted hosted = new Hosted(actorType, actor, budget);
        if (actors.putIfAbsent(actorId, hosted) != null) {
            hosted.mailbox.shutdown();
            throw new IllegalStateException(actorId + " is already hosted here");
        }
        stoppedBySupervisor.remove(actorId);
    }

    /**
     * Queues one message. A message for an actor its supervisor has stopped is
     * counted as a dead letter. Throws only if the actor was never hosted here.
     */
    @Override
    public void deliver(String actorId, String payload) {
        Hosted h = actors.get(actorId);
        if (h == null) {
            if (stoppedBySupervisor.containsKey(actorId)) {
                deadLetters.incrementAndGet();
                return;
            }
            throw new IllegalArgumentException(actorId + " is not hosted here");
        }
        h.pending.incrementAndGet();
        try {
            h.mailbox.execute(new Delivery(() -> process(actorId, h, payload)));
        } catch (RejectedExecutionException e) {
            // The actor stopped between the lookup and the enqueue.
            h.pending.decrementAndGet();
            deadLetters.incrementAndGet();
        }
    }

    /** Runs on the actor's own thread. */
    private void process(String actorId, Hosted h, String payload) {
        long start = System.nanoTime();
        try {
            h.actor.onMessage(payload);
        } catch (RuntimeException e) {
            h.failures.incrementAndGet();
            supervise(actorId, h, e);
        } finally {
            h.processingNanos.addAndGet(System.nanoTime() - start);
            h.processed.incrementAndGet();
            h.pending.decrementAndGet();
        }
    }

    /** Applies the supervision policy after a failed message. Runs on the actor's own thread. */
    private void supervise(String actorId, Hosted h, RuntimeException cause) {
        Supervision.Directive directive = supervision.directive();
        if (directive == Supervision.Directive.RESTART && !h.budget.tryAcquire()) {
            directive = Supervision.Directive.ESCALATE; // restart budget used up
        }
        switch (directive) {
            case RESTART -> {
                try {
                    h.actor = factories.get(h.type).get();
                } catch (RuntimeException e) {
                    stopAfterFailure(actorId, h, "could not create a fresh instance: " + e.getMessage());
                    return;
                }
                h.restarts.incrementAndGet();
                LOG.log(System.Logger.Level.WARNING, "restarted " + actorId + " after a failure", cause);
            }
            case STOP -> stopAfterFailure(actorId, h, "STOP policy after " + cause);
            case ESCALATE -> stopAfterFailure(actorId, h, "failure escalated to the node's root supervisor: " + cause);
        }
    }

    /**
     * Stops an actor from its own thread and counts what was still queued as
     * dead letters. The order matters: the reason is recorded first, then the
     * mailbox stops accepting work, then queued messages are counted, and only
     * then does the actor disappear from {@link #hosts}. So once hosts() is
     * false, every message sent before that point is already accounted for.
     */
    private void stopAfterFailure(String actorId, Hosted h, String reason) {
        if (actors.get(actorId) != h) {
            return; // already stopped by someone else
        }
        stoppedBySupervisor.put(actorId, reason);
        List<Runnable> queued = h.mailbox.shutdownNow();
        Thread.interrupted(); // shutdownNow interrupts this thread, which is finishing its last task
        int lost = 0;
        for (Runnable task : queued) {
            if (task instanceof Delivery) {
                lost++;
            } else if (task instanceof Future<?> query) {
                query.cancel(false); // a snapshot or inspect waiting behind the failure
            }
        }
        h.pending.addAndGet(-lost);
        deadLetters.addAndGet(lost);
        actors.remove(actorId, h);
        LOG.log(System.Logger.Level.WARNING, "stopped " + actorId + " (" + reason + "); "
            + lost + " queued messages counted as dead letters");
    }

    /** Mailbox depth, processed count, mean processing time, failures and restarts of a hosted actor. */
    public ActorMetrics metrics(String actorId) {
        Hosted h = require(actorId);
        long n = h.processed.get();
        double avg = n == 0 ? 0 : h.processingNanos.get() / 1_000.0 / n;
        return new ActorMetrics(h.pending.get(), n, avg, h.failures.get(), h.restarts.get());
    }

    /** Current mailbox depth of every actor hosted here. */
    public Map<String, Integer> mailboxDepths() {
        Map<String, Integer> depths = new HashMap<>();
        actors.forEach((id, h) -> depths.put(id, h.pending.get()));
        return depths;
    }

    /** Why the supervisor stopped this actor, or null if it did not. */
    public String stoppedReason(String actorId) {
        return stoppedBySupervisor.get(actorId);
    }

    /** Messages for actors stopped by their supervisor: queued at the time, or sent afterwards. */
    public long deadLetterCount() {
        return deadLetters.get();
    }

    @Override
    public boolean awaitIdle(String actorId, Duration timeout) throws InterruptedException {
        Hosted h = require(actorId);
        long deadline = System.nanoTime() + timeout.toNanos();
        while (h.pending.get() > 0) {
            if (System.nanoTime() >= deadline) {
                return false;
            }
            Thread.sleep(1);
        }
        return true;
    }

    @Override
    public String snapshot(String actorId) {
        return inspect(actorId, MigratableActor::snapshot);
    }

    /**
     * Runs a read-only query on the actor's own thread, after every message
     * already queued. Used for snapshots and for status in the node shell.
     */
    public <T> T inspect(String actorId, Function<MigratableActor, T> query) {
        Hosted h = require(actorId);
        Future<T> result;
        try {
            result = h.mailbox.submit(() -> query.apply(h.actor));
        } catch (RejectedExecutionException e) {
            throw new IllegalStateException(actorId + " has stopped", e);
        }
        try {
            return result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while inspecting " + actorId, e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("query failed on " + actorId, e.getCause());
        } catch (CancellationException e) {
            throw new IllegalStateException(actorId + " was stopped by its supervisor", e);
        }
    }

    @Override
    public void stop(String actorId) {
        stoppedBySupervisor.remove(actorId);
        Hosted h = actors.remove(actorId);
        if (h == null) {
            return;
        }
        h.mailbox.shutdown();
        try {
            h.mailbox.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private Hosted require(String actorId) {
        Hosted h = actors.get(actorId);
        if (h == null) {
            throw new IllegalArgumentException(actorId + " is not hosted here");
        }
        return h;
    }
}
