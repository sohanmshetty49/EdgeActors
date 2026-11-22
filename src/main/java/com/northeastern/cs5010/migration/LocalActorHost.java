package com.northeastern.cs5010.migration;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Runs migratable actors on this node. Each actor gets a single-threaded
 * mailbox backed by a virtual thread, so messages to one actor are processed
 * one at a time and in arrival order, and thousands of idle actors cost almost
 * nothing on a Raspberry Pi.
 */
public final class LocalActorHost implements ActorHost {

    private static final System.Logger LOG = System.getLogger(LocalActorHost.class.getName());

    private final Map<String, Supplier<? extends MigratableActor>> factories;
    private final Map<String, Hosted> actors = new ConcurrentHashMap<>();

    /**
     * A snapshot of one actor's metrics on this node.
     *
     * @param mailboxDepth messages queued or running right now
     * @param processed messages handled since the actor was installed here
     * @param averageProcessingMicros mean time spent in onMessage per message
     * @param failures messages whose onMessage threw
     */
    public record ActorMetrics(int mailboxDepth, long processed, double averageProcessingMicros, long failures) {
    }

    private static final class Hosted {
        final String type;
        final MigratableActor actor;
        final ExecutorService mailbox = Executors.newSingleThreadExecutor(Thread.ofVirtual().factory());
        final AtomicInteger pending = new AtomicInteger();
        final AtomicLong processed = new AtomicLong();
        final AtomicLong processingNanos = new AtomicLong();
        final AtomicLong failures = new AtomicLong();

        Hosted(String type, MigratableActor actor) {
            this.type = type;
            this.actor = actor;
        }
    }

    /** factories maps a type name such as "counter" to a constructor for that actor. */
    public LocalActorHost(Map<String, Supplier<? extends MigratableActor>> factories) {
        this.factories = Map.copyOf(factories);
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
        Hosted hosted = new Hosted(actorType, actor);
        if (actors.putIfAbsent(actorId, hosted) != null) {
            hosted.mailbox.shutdown();
            throw new IllegalStateException(actorId + " is already hosted here");
        }
    }

    @Override
    public void deliver(String actorId, String payload) {
        Hosted h = require(actorId);
        h.pending.incrementAndGet();
        h.mailbox.execute(() -> {
            long start = System.nanoTime();
            try {
                h.actor.onMessage(payload);
            } catch (RuntimeException e) {
                // One bad message must not kill the actor's mailbox thread.
                h.failures.incrementAndGet();
                LOG.log(System.Logger.Level.WARNING, actorId + " failed on a message", e);
            } finally {
                h.processingNanos.addAndGet(System.nanoTime() - start);
                h.processed.incrementAndGet();
                h.pending.decrementAndGet();
            }
        });
    }

    /** Mailbox depth, processed count, mean processing time and failures of a hosted actor. */
    public ActorMetrics metrics(String actorId) {
        Hosted h = require(actorId);
        long n = h.processed.get();
        double avg = n == 0 ? 0 : h.processingNanos.get() / 1_000.0 / n;
        return new ActorMetrics(h.pending.get(), n, avg, h.failures.get());
    }

    /** Current mailbox depth of every actor hosted here. */
    public Map<String, Integer> mailboxDepths() {
        Map<String, Integer> depths = new HashMap<>();
        actors.forEach((id, h) -> depths.put(id, h.pending.get()));
        return depths;
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
        Future<T> result = h.mailbox.submit(() -> query.apply(h.actor));
        try {
            return result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while inspecting " + actorId, e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("query failed on " + actorId, e.getCause());
        }
    }

    @Override
    public void stop(String actorId) {
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
