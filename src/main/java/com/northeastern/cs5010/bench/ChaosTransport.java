package com.northeastern.cs5010.bench;

import com.northeastern.cs5010.migration.HandoffMessage;
import com.northeastern.cs5010.transport.AsyncTransport;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * Wraps a real transport and injects network faults: message delay, lost
 * protocol messages, and network partitions.
 *
 * <ul>
 *   <li><b>Delay.</b> Each send waits a random time up to maxDelayMillis
 *       before it goes out. The wait happens in the sending thread, so
 *       messages to one node stay in order, as they do over one TCP
 *       connection.</li>
 *   <li><b>Lost protocol messages.</b> PREPARE, PREPARED, REJECT, COMMIT,
 *       COMMITTED, ABORT and QUERY are dropped silently with the given
 *       probability, as if a request or reply never arrived. FORWARD and
 *       DRAINED are part of the ordered data stream between two nodes, which
 *       TCP does not lose on a working connection, so they are not dropped
 *       here; a partition still stops them.</li>
 *   <li><b>Partition.</b> While the shared {@link Network} is partitioned,
 *       every send fails with an IOException, in both directions, like a
 *       connection to an unreachable host.</li>
 * </ul>
 */
public final class ChaosTransport implements AsyncTransport {

    /** The link between nodes. Both ends share one, so a partition cuts both directions. */
    public static final class Network {
        private volatile boolean partitioned;
        private final AtomicLong partitions = new AtomicLong();

        public void partition() {
            if (!partitioned) {
                partitions.incrementAndGet();
            }
            partitioned = true;
        }

        public void heal() {
            partitioned = false;
        }

        public boolean isPartitioned() {
            return partitioned;
        }

        /** Times the network was cut. */
        public long partitionCount() {
            return partitions.get();
        }
    }

    /** Request and reply messages: the protocol recovers from losing any of them. */
    private static final EnumSet<HandoffMessage.Type> DROPPABLE = EnumSet.of(
        HandoffMessage.Type.PREPARE, HandoffMessage.Type.PREPARED, HandoffMessage.Type.REJECT,
        HandoffMessage.Type.COMMIT, HandoffMessage.Type.COMMITTED, HandoffMessage.Type.ABORT,
        HandoffMessage.Type.QUERY);

    private final AsyncTransport inner;
    private final Network network;
    private final long maxDelayMillis;
    private final double dropRate;
    private final Random random;
    private final AtomicLong delayed = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();
    private final AtomicLong refused = new AtomicLong();

    /**
     * @param maxDelayMillis upper bound of the random delay per send; 0 for none
     * @param dropRate probability that a protocol message is lost, from 0 to 1
     * @param seed seeds the random delays and drops
     */
    public ChaosTransport(AsyncTransport inner, Network network, long maxDelayMillis, double dropRate, long seed) {
        this.inner = Objects.requireNonNull(inner, "inner");
        this.network = Objects.requireNonNull(network, "network");
        if (maxDelayMillis < 0) {
            throw new IllegalArgumentException("delay must not be negative");
        }
        if (!(dropRate >= 0 && dropRate <= 1)) {
            throw new IllegalArgumentException("drop rate must be between 0 and 1");
        }
        this.maxDelayMillis = maxDelayMillis;
        this.dropRate = dropRate;
        this.random = new Random(seed);
    }

    @Override
    public void send(String destination, String message) throws IOException {
        if (network.isPartitioned()) {
            refused.incrementAndGet();
            throw new IOException("network partitioned: cannot reach " + destination);
        }
        if (maxDelayMillis > 0) {
            long wait = nextLong(maxDelayMillis + 1);
            if (wait > 0) {
                delayed.incrementAndGet();
                try {
                    Thread.sleep(wait);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted while delayed", e);
                }
            }
            if (network.isPartitioned()) {
                // The link went down while this message was waiting.
                refused.incrementAndGet();
                throw new IOException("network partitioned: cannot reach " + destination);
            }
        }
        if (dropRate > 0 && isProtocolMessage(message) && nextDouble() < dropRate) {
            dropped.incrementAndGet();
            return;
        }
        inner.send(destination, message);
    }

    private static boolean isProtocolMessage(String message) {
        try {
            return DROPPABLE.contains(HandoffMessage.decode(message).type());
        } catch (IllegalArgumentException notHandoff) {
            return false;
        }
    }

    private long nextLong(long bound) {
        synchronized (random) {
            return (long) (random.nextDouble() * bound);
        }
    }

    private double nextDouble() {
        synchronized (random) {
            return random.nextDouble();
        }
    }

    /** Sends that were held back by a random delay. */
    public long delayedCount() {
        return delayed.get();
    }

    /** Protocol messages lost on purpose. */
    public long droppedCount() {
        return dropped.get();
    }

    /** Sends refused because the network was partitioned. */
    public long refusedCount() {
        return refused.get();
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
