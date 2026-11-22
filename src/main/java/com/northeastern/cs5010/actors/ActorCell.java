package com.northeastern.cs5010.actors;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Everything the system keeps for one live actor: its current instance, its
 * mailbox, its place in the supervision tree, and its restart budget.
 *
 * <p>The mailbox and ref survive restarts; only the instance is replaced. That
 * is why messages queued behind a failing message are still processed by the
 * fresh instance.
 *
 * <p>Locking rule: the instance is only touched while holding this cell's
 * monitor, and no code ever holds two cells' monitors at once. Supervision
 * decisions run after the failing receive has returned and released its
 * lock, so escalations from many children at the same time cannot deadlock.
 */
final class ActorCell {

    final ActorRef ref;
    final Class<? extends Actor> type;
    final ActorCell parent;
    final Supervision supervision;
    final RestartBudget budget;
    final Mailbox mailbox = new Mailbox();
    final Set<ActorCell> children = ConcurrentHashMap.newKeySet();
    final AtomicLong failures = new AtomicLong();
    final AtomicLong restarts = new AtomicLong();

    volatile boolean stopped;
    volatile boolean started;
    volatile Throwable lastFailure;
    volatile Thread thread;

    private Actor instance;

    ActorCell(ActorRef ref, Class<? extends Actor> type, ActorCell parent, Supervision supervision,
              RestartBudget budget) {
        this.ref = ref;
        this.type = type;
        this.parent = parent;
        this.supervision = supervision;
        this.budget = budget;
    }

    /** Makes the given instance current. Caller holds this cell's monitor or owns the cell exclusively. */
    void install(Actor fresh, ActorSystem system) {
        fresh.setSelf(ref);
        fresh.attach(system);
        instance = fresh;
    }

    Actor instance() {
        return instance;
    }

    /** Runs preStart on the current instance; returns the failure, if any. */
    synchronized RuntimeException start() {
        started = true;
        try {
            instance.preStart();
            return null;
        } catch (RuntimeException e) {
            return e;
        }
    }

    /** Delivers one message; returns the failure, if any, after releasing the lock. */
    synchronized RuntimeException process(Message message) {
        if (stopped) {
            return null;
        }
        try {
            instance.receive(message);
            return null;
        } catch (RuntimeException e) {
            return e;
        }
    }

    /**
     * Runs postStop on the final instance. Called by the actor's own runner
     * thread as it exits, so stopping another actor never takes its lock.
     */
    synchronized void finish() {
        if (!started) {
            return;
        }
        try {
            instance.postStop();
        } catch (RuntimeException e) {
            System.getLogger(ActorCell.class.getName())
                .log(System.Logger.Level.WARNING, "postStop failed in " + ref.getPath(), e);
        }
    }

    /** Wakes the runner thread so it notices the cell has stopped. */
    void wakeRunner() {
        Thread t = thread;
        if (t != null && t != Thread.currentThread()) {
            t.interrupt();
        }
    }
}
