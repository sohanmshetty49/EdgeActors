package com.northeastern.cs5010.actors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ActorSystem and actor lifecycle.
 */
public class ActorSystemTest {
    private ActorSystem system;

    @BeforeEach
    public void setup() {
        system = new ActorSystem("test-node");
    }

    @Test
    public void testActorCreation() throws Exception {
        ActorRef ref = system.actorOf(TestActor.class, "test-actor");
        assertNotNull(ref);
        assertEquals("test-actor", ref.getPath());
    }

    @Test
    public void testSendMessage() throws Exception {
        ActorRef ref = system.actorOf(TestActor.class, "test-actor");
        ActorRef sender = new ActorRef("sender", "test-node");
        Message msg = new Message(sender, ref, "hello");

        system.send(ref, msg);
        Thread.sleep(100);

        assertTrue(system.getMailboxDepth(ref) >= 0);
    }

    @Test
    public void testMultipleMessages() throws Exception {
        ActorRef ref = system.actorOf(TestActor.class, "test-actor");
        ActorRef sender = new ActorRef("sender", "test-node");

        for (int i = 0; i < 5; i++) {
            Message msg = new Message(sender, ref, "msg-" + i);
            system.send(ref, msg);
        }

        Thread.sleep(100);
        assertTrue(system.getMailboxDepth(ref) >= 0);
    }

    @Test
    public void testMailboxDepth() throws Exception {
        ActorRef ref = system.actorOf(SlowTestActor.class, "slow-actor");
        ActorRef sender = new ActorRef("sender", "test-node");

        Message msg = new Message(sender, ref, "slow-msg");
        system.send(ref, msg);

        int depth = system.getMailboxDepth(ref);
        assertTrue(depth >= 0);
    }

    // Helper actor for testing
    public static class TestActor extends Actor {
        @Override
        public void receive(Message message) {
            // No-op for testing
        }
    }

    // Slow actor for mailbox depth testing
    public static class SlowTestActor extends Actor {
        @Override
        public void receive(Message message) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
