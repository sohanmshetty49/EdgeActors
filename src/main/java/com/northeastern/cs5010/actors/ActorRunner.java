package com.northeastern.cs5010.actors;

/**
 * ActorRunner polls the mailbox and dispatches messages to the actor.
 * Runs in its own thread.
 */
public class ActorRunner implements Runnable {
    private final Actor actor;
    private final Mailbox mailbox;
    private volatile boolean running = true;

    public ActorRunner(Actor actor, Mailbox mailbox) {
        this.actor = actor;
        this.mailbox = mailbox;
    }

    @Override
    public void run() {
        try {
            while (running) {
                Message message = mailbox.dequeue();
                if (message != null) {
                    actor.receive(message);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void stop() {
        running = false;
    }
}
