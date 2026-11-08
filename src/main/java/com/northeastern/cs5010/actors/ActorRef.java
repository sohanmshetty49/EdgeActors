package com.northeastern.cs5010.actors;

import java.io.Serializable;
import java.util.UUID;
import java.util.Objects;

/**
 * Immutable reference to an actor.
 * Contains unique ID and location information.
 * Serializable for remote messaging.
 */
public class ActorRef implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String id;
    private final String path;
    private final String location;

    public ActorRef(String path, String location) {
        this.id = UUID.randomUUID().toString();
        this.path = path;
        this.location = location;
    }

    public String getId() {
        return id;
    }

    public String getPath() {
        return path;
    }

    public String getLocation() {
        return location;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ActorRef actorRef = (ActorRef) o;
        return Objects.equals(id, actorRef.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return String.format("ActorRef(%s, %s)", path, location);
    }
}
