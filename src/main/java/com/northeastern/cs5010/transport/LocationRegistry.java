package com.northeastern.cs5010.transport;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks where each actor lives, with an epoch that grows by one on every move.
 *
 * <p>Entries are immutable and every change goes through an atomic
 * compute call, so two threads can never interleave a read and a write on the
 * same actor. The epoch acts as a fencing token: a migration only commits if
 * the epoch it started from is still current.
 */
public class LocationRegistry {

    public enum Status {
        ACTIVE, MIGRATING, PAUSED
    }

    private record Entry(String location, long epoch, Status status) {
    }

    private final ConcurrentHashMap<String, Entry> registry = new ConcurrentHashMap<>();

    /** Register an actor at a location with epoch 0. */
    public void register(String actorId, String location) {
        register(actorId, location, 0);
    }

    /** Register an actor at a known epoch, used when a node learns of an actor or receives one. */
    public void register(String actorId, String location, long epoch) {
        registry.put(actorId, new Entry(location, epoch, Status.ACTIVE));
    }

    /** Get actor location, or null if unknown. */
    public String getLocation(String actorId) {
        Entry entry = registry.get(actorId);
        return entry != null ? entry.location() : null;
    }

    /** Get actor epoch, or -1 if unknown. */
    public long getEpoch(String actorId) {
        Entry entry = registry.get(actorId);
        return entry != null ? entry.epoch() : -1;
    }

    /** Update location and increment epoch. */
    public void updateLocation(String actorId, String newLocation) {
        registry.computeIfPresent(actorId,
            (id, e) -> new Entry(newLocation, e.epoch() + 1, e.status()));
    }

    /** Set actor status. No effect for an unknown actor. */
    public void setStatus(String actorId, Status status) {
        registry.computeIfPresent(actorId, (id, e) -> new Entry(e.location(), e.epoch(), status));
    }

    /** Get actor status, or null if unknown. */
    public Status getStatus(String actorId) {
        Entry entry = registry.get(actorId);
        return entry != null ? entry.status() : null;
    }

    /**
     * Atomically moves an actor from ACTIVE to MIGRATING, but only if it is
     * active at the expected location. Returns false otherwise, so two
     * migrations of the same actor can never both start.
     */
    public boolean beginMigration(String actorId, String expectedLocation) {
        boolean[] started = {false};
        registry.computeIfPresent(actorId, (id, e) -> {
            if (e.status() != Status.ACTIVE || !e.location().equals(expectedLocation)) {
                return e;
            }
            started[0] = true;
            return new Entry(e.location(), e.epoch(), Status.MIGRATING);
        });
        return started[0];
    }

    /**
     * Atomically commits a move: location becomes newLocation, epoch becomes
     * expectedEpoch + 1, status becomes ACTIVE. Fails if the epoch changed since
     * the migration started.
     */
    public boolean commitMigration(String actorId, long expectedEpoch, String newLocation) {
        boolean[] committed = {false};
        registry.computeIfPresent(actorId, (id, e) -> {
            if (e.epoch() != expectedEpoch) {
                return e;
            }
            committed[0] = true;
            return new Entry(newLocation, expectedEpoch + 1, Status.ACTIVE);
        });
        return committed[0];
    }

    /** Remove actor from registry. */
    public void remove(String actorId) {
        registry.remove(actorId);
    }
}
