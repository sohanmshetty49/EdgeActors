package com.northeastern.cs5010.actors;

/**
 * ActorRunner polls the mailbox and dispatches messages to the actor.
 * Runs on its own virtual thread.
 *
 * <p>When receive throws, the runner hands the failure to the system's
 * supervision logic and keeps going with the next message, unless the
 * supervisor stopped the actor.
 */
public class ActorRunner implements Runnable {
    private final ActorCell cell;
    private final ActorSystem system;

    ActorRunner(ActorCell cell, ActorSystem system) {
        this.cell = cell;
        this.system = system;
    }

    @Override
    public void run() {
        cell.thread = Thread.currentThread();
        if (cell.stopped) {
            return;
        }
        RuntimeException startFailure = cell.start();
        if (startFailure != null) {
            system.handleFailure(cell, startFailure);
        }
        try {
            while (!cell.stopped) {
                Message message = cell.mailbox.dequeue();
                if (cell.stopped) {
                    system.recordDeadLetter(); // arrived just as the actor stopped
                    break;
                }
                RuntimeException failure = cell.process(message);
                if (failure != null) {
                    system.handleFailure(cell, failure);
                }
            }
        } catch (InterruptedException e) {
            // stop() interrupts a runner that is waiting for mail
            Thread.currentThread().interrupt();
        } finally {
            cell.thread = null;
            cell.finish();
        }
    }

    /** Stops this actor; the runner exits after its current message. */
    public void stop() {
        cell.stopped = true;
        cell.wakeRunner();
    }
}
