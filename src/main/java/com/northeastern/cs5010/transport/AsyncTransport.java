package com.northeastern.cs5010.transport;

import java.io.IOException;
import java.util.function.BiConsumer;

/**
 * A network transport that can push inbound messages to a listener instead of
 * waiting to be polled. Code that needs callbacks can depend on this interface
 * and work the same over plain TCP and over TLS.
 */
public interface AsyncTransport extends Transport {

    /** Binds the server side and starts accepting peers. */
    void start() throws IOException;

    /** Narrows Transport.send: network transports only fail with IOException. */
    @Override
    void send(String destination, String message) throws IOException;

    /** Narrows Transport.receive: waiting can only be interrupted. */
    @Override
    String receive(String source) throws InterruptedException;

    /** The name this node puts on every outbound frame. */
    String nodeName();

    /**
     * Registers a callback invoked with (senderNode, payload) for every inbound
     * frame. Passing null switches back to polling through receive().
     */
    void setListener(BiConsumer<String, String> listener);
}
