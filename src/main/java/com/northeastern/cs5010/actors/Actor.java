package com.northeastern.cs5010.actors;

/**
 * Base class for all actors in the system.
 * Actors encapsulate private state and behavior.
 *
 * <p>The system calls receive, preStart and postStop one at a time, never
 * concurrently, so an actor's fields need no locking.
 */
public abstract class Actor {
    protected ActorRef self;
    private ActorSystem system;

    /**
     * Lifecycle method called when the actor receives a message.
     * Subclasses override this to define behavior. Throwing an exception
     * reports a failure to this actor's supervisor.
     */
    public abstract void receive(Message message);

    /** Called before the first message, and on each fresh instance after a restart. */
    public void preStart() {
    }

    /** Called once when this instance is discarded, on stop or before a restart replaces it. */
    public void postStop() {
    }

    public void setSelf(ActorRef self) {
        this.self = self;
    }

    public ActorRef getSelf() {
        return self;
    }

    void attach(ActorSystem system) {
        this.system = system;
    }

    /** The system this actor runs in, used to spawn children and send messages. */
    protected ActorSystem system() {
        return system;
    }
}
