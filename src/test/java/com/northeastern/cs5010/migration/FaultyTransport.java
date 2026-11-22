package com.northeastern.cs5010.migration;

import com.northeastern.cs5010.transport.AsyncTransport;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

/**
 * Test double that wraps a real transport and silently drops chosen outbound
 * handoff messages, to simulate a network that loses packets at the worst
 * possible moment.
 */
final class FaultyTransport implements AsyncTransport {

    private final AsyncTransport inner;
    private volatile Predicate<HandoffMessage> drop = m -> false;
    private final AtomicInteger dropped = new AtomicInteger();

    FaultyTransport(AsyncTransport inner) {
        this.inner = inner;
    }

    /** Drop every outbound message of this type until healed. */
    void dropAll(HandoffMessage.Type type) {
        drop = m -> m.type() == type;
    }

    /** Drop only the first n outbound messages of this type. */
    void dropFirst(HandoffMessage.Type type, int n) {
        AtomicInteger left = new AtomicInteger(n);
        drop = m -> m.type() == type && left.getAndDecrement() > 0;
    }

    void heal() {
        drop = m -> false;
    }

    int dropped() {
        return dropped.get();
    }

    @Override
    public void send(String destination, String message) throws IOException {
        HandoffMessage decoded;
        try {
            decoded = HandoffMessage.decode(message);
        } catch (IllegalArgumentException notHandoff) {
            inner.send(destination, message);
            return;
        }
        if (drop.test(decoded)) {
            dropped.incrementAndGet();
            return;
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
