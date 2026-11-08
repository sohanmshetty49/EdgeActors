package com.northeastern.cs5010.actors;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Mailbox for async message delivery.
 * Uses BlockingQueue for thread-safe message buffering.
 */
public class Mailbox {
    private static final int CAPACITY = 1000;
    private final BlockingQueue<Message> queue;

    public Mailbox() {
        this.queue = new LinkedBlockingQueue<>(CAPACITY);
    }

    /**
     * Enqueue a message for delivery.
     * Blocks if mailbox is full.
     */
    public void enqueue(Message message) throws InterruptedException {
        queue.put(message);
    }

    /**
     * Dequeue a message.
     * Blocks until a message is available.
     */
    public Message dequeue() throws InterruptedException {
        return queue.take();
    }

    /**
     * Non-blocking dequeue (returns null if empty).
     */
    public Message poll() {
        return queue.poll();
    }

    /**
     * Get current queue depth.
     */
    public int depth() {
        return queue.size();
    }

    /**
     * Check if mailbox is empty.
     */
    public boolean isEmpty() {
        return queue.isEmpty();
    }
}
