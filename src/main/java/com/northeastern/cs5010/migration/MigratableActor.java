package com.northeastern.cs5010.migration;

/**
 * An actor whose private state can be captured on one node and rebuilt on
 * another. The host guarantees all three methods run on the actor's own
 * thread, one at a time, so implementations need no locking.
 */
public interface MigratableActor {

    /** Handles one message. */
    void onMessage(String payload);

    /** Serializes the full private state. Called only while the actor is idle. */
    String snapshot();

    /** Rebuilds private state from a snapshot taken on another node. */
    void restore(String state);
}
