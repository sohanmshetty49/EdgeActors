package com.northeastern.cs5010.actors;

/**
 * Base class for all actors in the system.
 * Actors encapsulate private state and behavior.
 */
public abstract class Actor {
    protected ActorRef self;

    /**
     * Lifecycle method called when the actor receives a message.
     * Subclasses override this to define behavior.
     */
    public abstract void receive(Message message);

    public void setSelf(ActorRef self) {
        this.self = self;
    }

    public ActorRef getSelf() {
        return self;
    }
}
