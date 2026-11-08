package com.northeastern.cs5010.examples;

import com.northeastern.cs5010.actors.Actor;
import com.northeastern.cs5010.actors.Message;

/**
 * Simple echo actor for end-to-end testing.
 */
public class EchoActor extends Actor {
    @Override
    public void receive(Message message) {
        System.out.println("[EchoActor] Received: " + message.getPayload());
    }
}
