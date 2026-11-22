package com.northeastern.cs5010.actors;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Per-actor processing count and mean processing time (FR12). */
class ProcessingMetricsTest {

    private final ActorSystem system = new ActorSystem("node");

    @AfterEach
    void tearDown() {
        system.shutdown();
    }

    /** Sleeps 2 ms on "slow" and throws on "fail". */
    public static class Timed extends Actor {
        @Override
        public void receive(Message message) {
            if ("fail".equals(message.getPayload())) {
                throw new IllegalStateException("fail");
            }
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    void countsMessagesAndMeasuresTimeIncludingFailures() throws Exception {
        ActorRef a = system.actorOf(Timed.class, "a", Supervision.restart(10, Duration.ofMinutes(1)));
        assertEquals(0, system.processedCount(a));
        assertEquals(0.0, system.averageProcessingMicros(a), 1e-9);
        ActorRef sender = new ActorRef("client", "node");
        for (int i = 0; i < 5; i++) {
            system.send(a, new Message(sender, a, "slow"));
        }
        system.send(a, new Message(sender, a, "fail"));
        long deadline = System.currentTimeMillis() + 5_000;
        while (system.processedCount(a) < 6 && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(6, system.processedCount(a));
        // Five messages slept at least 2 ms each, spread over six messages.
        assertTrue(system.averageProcessingMicros(a) >= 5 * 2_000.0 / 6, "mean " + system.averageProcessingMicros(a));
        assertEquals(1, system.failureCount(a));
    }

    @Test
    void unknownActorsReportZero() {
        ActorRef ghost = new ActorRef("ghost", "node");
        assertEquals(0, system.processedCount(ghost));
        assertEquals(0.0, system.averageProcessingMicros(ghost), 1e-9);
    }
}
