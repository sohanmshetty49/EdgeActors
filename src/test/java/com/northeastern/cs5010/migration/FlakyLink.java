package com.northeastern.cs5010.migration;

import com.northeastern.cs5010.transport.AsyncTransport;
import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;

/**
 * Test double that wraps a real transport and makes chosen sends fail with an
 * IOException, the way a send fails while a link is down.
 */
final class FlakyLink implements AsyncTransport {

    private final AsyncTransport inner;
    /** (destination address, message type or null for non-protocol text) -> fail? */
    private volatile BiPredicate<String, HandoffMessage.Type> failing = (d, t) -> false;

    FlakyLink(AsyncTransport inner) {
        this.inner = inner;
    }

    void failWhen(BiPredicate<String, HandoffMessage.Type> rule) {
        failing = rule;
    }

    void heal() {
        failing = (d, t) -> false;
    }

    @Override
    public void send(String destination, String message) throws IOException {
        HandoffMessage.Type type;
        try {
            type = HandoffMessage.decode(message).type();
        } catch (IllegalArgumentException notHandoff) {
            type = null;
        }
        if (failing.test(destination, type)) {
            throw new IOException("link down (test)");
        }
        inner.send(destination, message);
    }

    @Override
    public String receive(String source) throws InterruptedException {
        return inner.receive(source);
    }

    @Override
    public boolean isConnected() {
        return inner.isConnected();
    }

    @Override
    public void close() {
        inner.close();
    }

    @Override
    public void start() throws IOException {
        inner.start();
    }

    @Override
    public String nodeName() {
        return inner.nodeName();
    }

    @Override
    public void setListener(BiConsumer<String, String> listener) {
        inner.setListener(listener);
    }
}
