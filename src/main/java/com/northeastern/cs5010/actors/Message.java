package com.northeastern.cs5010.actors;

import java.io.Serializable;

/**
 * Message envelope with sender, recipient, and payload.
 * Tracks timestamp for ordering guarantees.
 */
public class Message implements Serializable {
    private static final long serialVersionUID = 1L;

    private final ActorRef sender;
    private final ActorRef recipient;
    private final Object payload;
    private final long timestamp;

    public Message(ActorRef sender, ActorRef recipient, Object payload) {
        this.sender = sender;
        this.recipient = recipient;
        this.payload = payload;
        this.timestamp = System.currentTimeMillis();
    }

    public ActorRef getSender() {
        return sender;
    }

    public ActorRef getRecipient() {
        return recipient;
    }

    public Object getPayload() {
        return payload;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return String.format("Message(from=%s, to=%s, payload=%s, ts=%d)",
            sender, recipient, payload, timestamp);
    }
}
