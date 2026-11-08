package com.northeastern.cs5010.transport;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks actor locations across nodes with epoch numbers.
 */
public class LocationRegistry {

    public enum Status {
        ACTIVE, MIGRATING, PAUSED
    }

    private static class Entry {
        String location;
        long epoch;
        Status status;

        Entry(String location, long epoch, Status status) {
            this.location = location;
            this.epoch = epoch;
            this.status = status;
        }
    }

    private final ConcurrentHashMap<String, Entry> registry = new ConcurrentHashMap<>();

    /**
     * Register an actor at a location.
     */
    public void register(String actorId, String location) {
        registry.put(actorId, new Entry(location, 0, Status.ACTIVE));
    }

    /**
     * Get actor location.
     */
    public String getLocation(String actorId) {
        Entry entry = registry.get(actorId);
        return entry != null ? entry.location : null;
    }

    /**
     * Get actor epoch.
     */
    public long getEpoch(String actorId) {
        Entry entry = registry.get(actorId);
        return entry != null ? entry.epoch : -1;
    }

    /**
     * Update location and increment epoch.
     */
    public void updateLocation(String actorId, String newLocation) {
        Entry entry = registry.get(actorId);
        if (entry != null) {
            entry.location = newLocation;
            entry.epoch++;
        }
    }

    /**
     * Set actor status.
     */
    public void setStatus(String actorId, Status status) {
        Entry entry = registry.get(actorId);
        if (entry != null) {
            entry.status = status;
        }
    }

    /**
     * Get actor status.
     */
    public Status getStatus(String actorId) {
        Entry entry = registry.get(actorId);
        return entry != null ? entry.status : null;
    }

    /**
     * Remove actor from registry.
     */
    public void remove(String actorId) {
        registry.remove(actorId);
    }
}
