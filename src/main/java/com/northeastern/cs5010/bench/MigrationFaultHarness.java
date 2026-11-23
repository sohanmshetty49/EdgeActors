package com.northeastern.cs5010.bench;

import com.northeastern.cs5010.migration.LocalActorHost;
import com.northeastern.cs5010.migration.MigrationManager;
import com.northeastern.cs5010.transport.LocationRegistry;
import com.northeastern.cs5010.transport.TCPTransport;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Randomized live migration runs under network faults, checking the three
 * handoff invariants on every run.
 *
 * <p>Each run starts two real nodes ("pi" and "cloud") talking over TCP on
 * this machine through {@link ChaosTransport}. A client streams numbered
 * messages to a {@link SequenceRecorder} actor through the Pi while, at random
 * moments, the actor is migrated to the cloud (and sometimes back), protocol
 * messages are lost, sends are delayed, and the network is partitioned for a
 * while. Migrations left IN_DOUBT are resolved once the network heals.
 *
 * <p>Checked per run:
 * <ul>
 *   <li><b>I1 no loss:</b> every message whose send succeeded reached the
 *       actor. A send that failed with an IOException was reported to the
 *       client and does not count as lost.</li>
 *   <li><b>I2 no duplication:</b> no number arrived twice, and no message
 *       reported as failed arrived anyway.</li>
 *   <li><b>Order:</b> numbers arrived in increasing order.</li>
 *   <li><b>I3 single live location:</b> a monitor thread samples both nodes
 *       throughout the run and never sees two active copies at once.</li>
 * </ul>
 */
public final class MigrationFaultHarness {

    private static final String ACTOR = "w";

    /** Short timeouts so a run finishes quickly even when replies are lost. */
    static final MigrationManager.Config CONFIG = new MigrationManager.Config(
        Duration.ofMillis(300), 3, Duration.ofSeconds(2), Duration.ofSeconds(3));

    /**
     * @param runs number of randomized runs
     * @param messages numbered messages the client sends per run
     * @param maxDelayMillis upper bound of the random delay added to each send
     * @param dropRate probability that a protocol message is lost
     * @param partitionChance probability that a run includes a network partition
     * @param seed seeds every random choice
     */
    public record Settings(int runs, int messages, long maxDelayMillis, double dropRate, double partitionChance,
                           long seed) {
        public Settings {
            if (runs < 1 || messages < 1) {
                throw new IllegalArgumentException("runs and messages must be at least 1");
            }
            if (maxDelayMillis < 0) {
                throw new IllegalArgumentException("delay must not be negative");
            }
            if (!(dropRate >= 0 && dropRate <= 1) || !(partitionChance >= 0 && partitionChance <= 1)) {
                throw new IllegalArgumentException("drop rate and partition chance must be between 0 and 1");
            }
        }
    }

    private MigrationFaultHarness() {
    }

    public static MigrationFaultResult run(Settings settings) throws Exception {
        SplittableRandom random = new SplittableRandom(settings.seed());
        MigrationFaultResult.Builder total = new MigrationFaultResult.Builder(settings);
        long start = System.nanoTime();
        for (int i = 0; i < settings.runs(); i++) {
            runOnce(settings, random.split(), total);
        }
        return total.build((System.nanoTime() - start) / 1_000_000);
    }

    /** One node of a run. */
    private static final class Node {
        final String name;
        final TCPTransport tcp;
        final ChaosTransport chaos;
        final LocationRegistry registry = new LocationRegistry();
        final LocalActorHost host = new LocalActorHost(Map.of(SequenceRecorder.TYPE, SequenceRecorder::new));
        MigrationManager manager;

        Node(String name, ChaosTransport.Network network, Settings s, long seed) throws IOException {
            this.name = name;
            this.tcp = new TCPTransport(name, 0);
            tcp.start();
            this.chaos = new ChaosTransport(tcp, network, s.maxDelayMillis(), s.dropRate(), seed);
        }

        /**
         * True if this node runs an active copy that accepts messages; epoch in
         * out[0]. The registry fields are read one at a time, so the read is
         * repeated until location and epoch did not change in between; a
         * half-updated view could otherwise look like a second live copy.
         */
        boolean live(long[] epoch) {
            while (true) {
                long before = registry.getEpoch(ACTOR);
                String location = registry.getLocation(ACTOR);
                LocationRegistry.Status status = registry.getStatus(ACTOR);
                boolean hosted = host.hosts(ACTOR);
                if (before == registry.getEpoch(ACTOR) && java.util.Objects.equals(location, registry.getLocation(ACTOR))) {
                    epoch[0] = before;
                    return hosted && name.equals(location) && status == LocationRegistry.Status.ACTIVE;
                }
            }
        }

        void close() {
            if (manager != null) {
                manager.close();
            }
            tcp.close();
        }
    }

    private static void runOnce(Settings s, SplittableRandom random, MigrationFaultResult.Builder total)
            throws Exception {
        ChaosTransport.Network network = new ChaosTransport.Network();
        Node pi = new Node("pi", network, s, random.nextLong());
        Node cloud = new Node("cloud", network, s, random.nextLong());
        try {
            pi.manager = new MigrationManager(pi.host, pi.registry, pi.chaos,
                Map.of("cloud", "127.0.0.1:" + cloud.tcp.boundPort()), CONFIG);
            cloud.manager = new MigrationManager(cloud.host, cloud.registry, cloud.chaos,
                Map.of("pi", "127.0.0.1:" + pi.tcp.boundPort()), CONFIG);
            pi.manager.spawn(ACTOR, SequenceRecorder.TYPE);

            BitSet accepted = new BitSet();
            BitSet reported = new BitSet();
            AtomicInteger violations = new AtomicInteger();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            List<MigrationManager.Result> results = new ArrayList<>();
            AtomicInteger resolved = new AtomicInteger();
            AtomicInteger unresolved = new AtomicInteger();
            long moveDelay = random.nextLong(40);
            boolean moveBack = random.nextBoolean();
            long backDelay = random.nextLong(40);
            boolean partition = random.nextDouble() < s.partitionChance();
            long partitionAt = random.nextLong(60);
            long partitionFor = 30 + random.nextLong(270);
            Flag done = new Flag();

            Thread client = thread("client", failure, () -> {
                for (int seq = 1; seq <= s.messages(); seq++) {
                    try {
                        pi.manager.send(ACTOR, Integer.toString(seq));
                        synchronized (accepted) {
                            accepted.set(seq);
                        }
                    } catch (IOException e) {
                        synchronized (reported) {
                            reported.set(seq);
                        }
                    }
                    if (seq % 10 == 0) {
                        Thread.sleep(1);
                    }
                }
            });
            Thread mover = thread("mover", failure, () -> {
                Thread.sleep(moveDelay);
                record(results, move(pi, cloud, resolved, unresolved));
                if (moveBack) {
                    Thread.sleep(backDelay);
                    Node from = cloud.live(new long[1]) ? cloud : pi;
                    Node to = from == cloud ? pi : cloud;
                    record(results, move(from, to, resolved, unresolved));
                }
            });
            Thread partitioner = thread("partitioner", failure, () -> {
                if (partition) {
                    Thread.sleep(partitionAt);
                    network.partition();
                    Thread.sleep(partitionFor);
                    network.heal();
                }
            });
            Thread monitor = thread("monitor", failure, () -> {
                long[] e1 = new long[1];
                long[] e2 = new long[1];
                long[] e3 = new long[1];
                boolean flip = false;
                while (!done.value) {
                    Node a = flip ? cloud : pi;
                    Node b = flip ? pi : cloud;
                    flip = !flip;
                    // Read a, b, a: a copy that was live at both reads of a with the
                    // same epoch was live the whole time, so b live in between is a
                    // real overlap, not two moves seen out of step.
                    if (a.live(e1) && b.live(e2) && a.live(e3) && e1[0] == e3[0]) {
                        violations.incrementAndGet();
                    }
                    Thread.onSpinWait();
                }
            });
            monitor.start();
            client.start();
            mover.start();
            partitioner.start();
            client.join();
            mover.join();
            partitioner.join();
            network.heal();
            // Settle anything still in doubt now that the network is back.
            for (Node n : List.of(pi, cloud)) {
                if (n.manager.isInDoubt(ACTOR)) {
                    MigrationManager.Result r = settle(n, n.manager.resolve(ACTOR), resolved, unresolved);
                    record(results, r);
                }
            }
            SequenceRecorder state = settledState(pi, cloud);
            done.value = true;
            monitor.join();
            if (failure.get() != null) {
                throw new IllegalStateException("fault run failed", failure.get());
            }

            BitSet seen = state == null ? new BitSet() : state.seen();
            BitSet lost = (BitSet) accepted.clone();
            lost.andNot(seen);
            BitSet phantom = (BitSet) reported.clone();
            phantom.and(seen);
            total.add(s.messages(), accepted.cardinality(), reported.cardinality(), seen.cardinality(),
                lost.cardinality(), state == null ? 0 : state.duplicates(), phantom.cardinality(),
                state == null ? 0 : state.outOfOrder(), violations.get() + (state == null ? 1 : 0), results,
                resolved.get(), unresolved.get(), network.partitionCount(),
                pi.chaos.droppedCount() + cloud.chaos.droppedCount(),
                pi.chaos.delayedCount() + cloud.chaos.delayedCount(),
                pi.chaos.refusedCount() + cloud.chaos.refusedCount(),
                pi.manager.droppedCount() + cloud.manager.droppedCount());
        } finally {
            pi.close();
            cloud.close();
        }
    }

    /** Waits for the one live copy to finish its mailbox and stop changing, then returns its state. */
    private static SequenceRecorder settledState(Node pi, Node cloud) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        String previous = null;
        long stableSince = 0;
        while (System.currentTimeMillis() < deadline) {
            Node live = pi.live(new long[1]) ? pi : cloud.live(new long[1]) ? cloud : null;
            if (live != null && live.host.awaitIdle(ACTOR, Duration.ofSeconds(1))) {
                String snap;
                try {
                    snap = live.host.snapshot(ACTOR);
                } catch (IllegalArgumentException | IllegalStateException movedMeanwhile) {
                    continue;
                }
                long now = System.currentTimeMillis();
                if (!snap.equals(previous)) {
                    previous = snap;
                    stableSince = now;
                } else if (now - stableSince >= 200) {
                    SequenceRecorder state = new SequenceRecorder();
                    state.restore(snap);
                    return state;
                }
            }
            Thread.sleep(10);
        }
        return null; // no live copy: counted as a single-location failure
    }

    private static MigrationManager.Result move(Node from, Node to, AtomicInteger resolved,
                                                AtomicInteger unresolved) throws InterruptedException {
        return settle(from, from.manager.migrate(ACTOR, to.name), resolved, unresolved);
    }

    /** Retries resolve until an IN_DOUBT migration is decided, for up to 10 seconds. */
    private static MigrationManager.Result settle(Node node, MigrationManager.Result r, AtomicInteger resolved,
                                                  AtomicInteger unresolved) throws InterruptedException {
        if (r.outcome() != MigrationManager.Outcome.IN_DOUBT) {
            return r;
        }
        long deadline = System.currentTimeMillis() + 10_000;
        while (r.outcome() == MigrationManager.Outcome.IN_DOUBT && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
            r = node.manager.resolve(ACTOR);
        }
        if (r.outcome() == MigrationManager.Outcome.IN_DOUBT) {
            unresolved.incrementAndGet();
        } else {
            resolved.incrementAndGet();
        }
        return r;
    }

    private static void record(List<MigrationManager.Result> results, MigrationManager.Result r) {
        synchronized (results) {
            results.add(r);
        }
    }

    private interface Body {
        void run() throws Exception;
    }

    private static Thread thread(String name, AtomicReference<Throwable> failure, Body body) {
        Thread t = new Thread(() -> {
            try {
                body.run();
            } catch (Throwable e) {
                failure.compareAndSet(null, e);
            }
        }, "fault-" + name);
        t.setDaemon(true);
        return t;
    }

    /** A flag the monitor polls. */
    private static final class Flag {
        volatile boolean value;
    }
}
