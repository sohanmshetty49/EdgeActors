package com.northeastern.cs5010.actors;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * ActorSystem manages actor lifecycle and message routing.
 */
public class ActorSystem {
    private final ConcurrentHashMap<String, Actor> actors;
    private final ConcurrentHashMap<String, Mailbox> mailboxes;
    private final ConcurrentHashMap<String, ActorRunner> runners;
    private final ExecutorService executor;
    private final String nodeName;

    public ActorSystem(String nodeName) {
        this.nodeName = nodeName;
        this.actors = new ConcurrentHashMap<>();
        this.mailboxes = new ConcurrentHashMap<>();
        this.runners = new ConcurrentHashMap<>();
        this.executor = Executors.newFixedThreadPool(10);
    }

    /**
     * Create and start a new actor.
     */
    public ActorRef actorOf(Class<? extends Actor> actorClass, String name) throws Exception {
        Actor actor = actorClass.getDeclaredConstructor().newInstance();
        ActorRef ref = new ActorRef(name, nodeName);

        actor.setSelf(ref);
        actors.put(ref.getId(), actor);

        Mailbox mailbox = new Mailbox();
        mailboxes.put(ref.getId(), mailbox);

        ActorRunner runner = new ActorRunner(actor, mailbox);
        runners.put(ref.getId(), runner);
        executor.execute(runner);

        return ref;
    }

    /**
     * Send a message to an actor.
     */
    public void send(ActorRef recipient, Message message) throws InterruptedException {
        Mailbox mailbox = mailboxes.get(recipient.getId());
        if (mailbox != null) {
            mailbox.enqueue(message);
        }
    }

    /**
     * Get mailbox depth for monitoring.
     */
    public int getMailboxDepth(ActorRef ref) {
        Mailbox mailbox = mailboxes.get(ref.getId());
        return mailbox != null ? mailbox.depth() : 0;
    }

    /**
     * Shutdown the actor system.
     */
    public void shutdown() {
        executor.shutdown();
    }
}
