package com.northeastern.cs5010.transport;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * Shared behaviour for transports that move length-prefixed frames.
 *
 * <p>Frame layout: a 4-byte big-endian length, then the UTF-8 bytes of
 * {@code sender + '\n' + payload}. Carrying the sender inside the frame is what
 * lets {@link #receive(String)} keep one ordered inbox per peer.
 *
 * <p>Subclasses only deal with sockets. They call {@link #deliver(String)} for
 * each complete frame and {@link #recordSent()} after each successful write.
 */
public abstract class FramedTransport implements AsyncTransport {

    /** Upper bound on one frame body, guards against corrupt length prefixes. */
    public static final int MAX_FRAME = 1 << 20;

    static final long DEFAULT_RECEIVE_TIMEOUT_MS = 5_000;
    private static final int INBOX_CAPACITY = 1_000;
    private static final System.Logger LOG = System.getLogger(FramedTransport.class.getName());

    protected final String nodeName;
    private final Map<String, BlockingQueue<String>> inboxes = new ConcurrentHashMap<>();
    private final AtomicLong framesSent = new AtomicLong();
    private final AtomicLong framesReceived = new AtomicLong();
    private volatile BiConsumer<String, String> listener;

    protected FramedTransport(String nodeName) {
        if (nodeName == null || nodeName.isEmpty() || nodeName.indexOf('\n') >= 0) {
            throw new IllegalArgumentException("nodeName must be non-empty and must not contain newline");
        }
        this.nodeName = nodeName;
    }

    @Override
    public String nodeName() {
        return nodeName;
    }

    @Override
    public void setListener(BiConsumer<String, String> listener) {
        this.listener = listener;
    }

    /** Encodes one frame: [4-byte length][UTF-8 of "sender\npayload"]. */
    static byte[] frame(String sender, String message) {
        byte[] body = (sender + "\n" + message).getBytes(StandardCharsets.UTF_8);
        if (body.length > MAX_FRAME) {
            throw new IllegalArgumentException("message too large: " + body.length);
        }
        ByteBuffer out = ByteBuffer.allocate(4 + body.length);
        out.putInt(body.length).put(body);
        return out.array();
    }

    /**
     * Hands one decoded frame body to the listener, or to the sender's inbox
     * when no listener is set. Throws IllegalStateException for a frame with no
     * sender line, so the caller can drop that connection.
     */
    protected void deliver(String frame) {
        int split = frame.indexOf('\n');
        if (split <= 0) {
            throw new IllegalStateException("frame has no sender");
        }
        String sender = frame.substring(0, split);
        String payload = frame.substring(split + 1);
        framesReceived.incrementAndGet();
        BiConsumer<String, String> current = listener;
        if (current != null) {
            try {
                current.accept(sender, payload);
            } catch (RuntimeException e) {
                // A bug in the listener must not take the connection down with it.
                LOG.log(System.Logger.Level.WARNING, "listener failed on frame from " + sender, e);
            }
            return;
        }
        try {
            // Blocks the reading thread when the inbox is full: this is backpressure.
            inboxFor(sender).put(payload);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    protected void recordSent() {
        framesSent.incrementAndGet();
    }

    private BlockingQueue<String> inboxFor(String sender) {
        return inboxes.computeIfAbsent(sender, k -> new LinkedBlockingQueue<>(INBOX_CAPACITY));
    }

    @Override
    public String receive(String source) throws InterruptedException {
        return receive(source, DEFAULT_RECEIVE_TIMEOUT_MS);
    }

    /** Waits up to timeoutMillis for the next message from source; null on timeout. */
    public String receive(String source, long timeoutMillis) throws InterruptedException {
        return inboxFor(source).poll(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    public long framesSent() {
        return framesSent.get();
    }

    public long framesReceived() {
        return framesReceived.get();
    }

    /** Splits "host:port" and validates the port. */
    static HostPort parseAddress(String destination) {
        int colon = destination == null ? -1 : destination.lastIndexOf(':');
        if (colon <= 0 || colon == destination.length() - 1) {
            throw new IllegalArgumentException("address must look like host:port, got " + destination);
        }
        int port;
        try {
            port = Integer.parseInt(destination.substring(colon + 1));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("bad port in " + destination, e);
        }
        if (port < 1 || port > 65_535) {
            throw new IllegalArgumentException("port out of range in " + destination);
        }
        return new HostPort(destination.substring(0, colon), port);
    }

    record HostPort(String host, int port) {
    }

    /** Reassembles frames from arbitrary TCP chunks. */
    static final class FrameDecoder {
        private ByteBuffer acc = ByteBuffer.allocate(0);

        void feed(ByteBuffer in) {
            ByteBuffer grown = ByteBuffer.allocate(acc.remaining() + in.remaining());
            grown.put(acc).put(in).flip();
            acc = grown;
        }

        /** Returns the next complete frame body, or null if more bytes are needed. */
        String next() {
            if (acc.remaining() < 4) {
                return null;
            }
            acc.mark();
            int len = acc.getInt();
            if (len < 0 || len > MAX_FRAME) {
                throw new IllegalStateException("bad frame length " + len);
            }
            if (acc.remaining() < len) {
                acc.reset();
                return null;
            }
            byte[] body = new byte[len];
            acc.get(body);
            return new String(body, StandardCharsets.UTF_8);
        }
    }
}
