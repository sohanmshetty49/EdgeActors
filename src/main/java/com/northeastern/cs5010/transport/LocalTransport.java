package com.northeastern.cs5010.transport;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * MVP transport for single-JVM testing.
 * Uses in-memory queues for message passing.
 */
public class LocalTransport implements Transport {
    private static final ConcurrentHashMap<String, BlockingQueue<String>> channels
        = new ConcurrentHashMap<>();

    private final String nodeName;
    private boolean connected;

    public LocalTransport(String nodeName) {
        this.nodeName = nodeName;
        this.connected = true;
        channels.putIfAbsent(nodeName, new LinkedBlockingQueue<>());
    }

    @Override
    public void send(String destination, String message) {
        BlockingQueue<String> channel = channels.get(destination);
        if (channel != null) {
            try {
                channel.put(message);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public String receive(String source) throws InterruptedException {
        BlockingQueue<String> channel = channels.get(nodeName);
        if (channel != null) {
            return channel.take();
        }
        return null;
    }

    @Override
    public boolean isConnected() {
        return connected;
    }

    @Override
    public void close() {
        connected = false;
    }
}
