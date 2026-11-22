package com.northeastern.cs5010.actors;

/**
 * The failure a supervisor receives when one of its children escalates. It
 * names the child that failed and keeps the child's error as the cause.
 */
public final class EscalatedFailure extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final transient ActorRef child;

    public EscalatedFailure(ActorRef child, Throwable cause) {
        super("child " + child.getPath() + " escalated: " + cause, cause);
        this.child = child;
    }

    public ActorRef child() {
        return child;
    }
}
