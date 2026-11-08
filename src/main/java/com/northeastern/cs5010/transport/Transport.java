package com.northeastern.cs5010.transport;

/**
 * Interface for inter-node transport.
 * Implementations handle messaging between nodes.
 */
public interface Transport {
    /**
     * Send a message to a remote node.
     */
    void send(String destination, String message) throws Exception;

    /**
     * Receive a message from a remote node.
     */
    String receive(String source) throws Exception;

    /**
     * Check if transport is connected.
     */
    boolean isConnected();

    /**
     * Close the transport.
     */
    void close();
}
