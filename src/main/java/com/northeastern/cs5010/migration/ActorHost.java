package com.northeastern.cs5010.migration;

import java.time.Duration;

/**
 * What the migration protocol needs from the runtime that runs actors on a
 * node. MigrationManager depends only on this interface, so the protocol can
 * be tested with a small in-memory host and later plugged into the full
 * ActorSystem through an adapter, without changing the protocol code.
 */
public interface ActorHost {

    boolean hosts(String actorId);

    /** The actor's type name, or null if it is not hosted here. */
    String typeOf(String actorId);

    /** True if this node knows how to create actors of the given type. */
    boolean canInstall(String actorType);

    /**
     * Waits until the actor has no queued or running messages. Returns false
     * on timeout. The caller must already be holding new messages back.
     */
    boolean awaitIdle(String actorId, Duration timeout) throws InterruptedException;

    /** Captures the actor's state. Only valid while the actor is idle. */
    String snapshot(String actorId);

    /** Creates and starts an actor. A null state means a fresh actor. */
    void install(String actorId, String actorType, String state);

    /** Queues one message for the actor. */
    void deliver(String actorId, String payload);

    /** Stops the actor and frees its resources. No effect if not hosted. */
    void stop(String actorId);
}
