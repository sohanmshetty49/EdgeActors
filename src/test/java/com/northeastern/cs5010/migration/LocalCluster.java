package com.northeastern.cs5010.migration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.northeastern.cs5010.transport.LocationRegistry;
import com.northeastern.cs5010.transport.TCPTransport;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Several real nodes in one JVM, talking over real TCP sockets on free ports.
 * Each node's transport is wrapped in a FaultyTransport for fault injection.
 */
final class LocalCluster implements AutoCloseable {

    /** Short timeouts so failure tests finish quickly. */
    static final MigrationManager.Config FAST = new MigrationManager.Config(
        Duration.ofSeconds(1), 3, Duration.ofMillis(500), Duration.ofSeconds(10));

    static final class Node {
        final String name;
        final TCPTransport transport;
        final FaultyTransport faulty;
        final LocationRegistry registry = new LocationRegistry();
        final LocalActorHost host;
        MigrationManager manager;

        Node(String name, TCPTransport transport, LocalActorHost host) {
            this.name = name;
            this.transport = transport;
            this.faulty = new FaultyTransport(transport);
            this.host = host;
        }

        long count(String actorId) {
            return host.inspect(actorId, a -> ((CounterActor) a).count());
        }

        CounterActor counter(String actorId) {
            // Copy the state on the actor's thread so the test reads a consistent view.
            return host.inspect(actorId, a -> {
                CounterActor copy = new CounterActor();
                copy.restore(a.snapshot());
                return copy;
            });
        }

        /** Polls until the counter actor here has processed expected messages. */
        void awaitCount(String actorId, long expected) throws InterruptedException {
            long deadline = System.currentTimeMillis() + 10_000;
            long seen = -1;
            while (System.currentTimeMillis() < deadline) {
                if (host.hosts(actorId)) {
                    seen = count(actorId);
                    if (seen == expected) {
                        return;
                    }
                }
                Thread.sleep(5);
            }
            fail(actorId + " on " + name + " expected count " + expected + " but saw " + seen);
        }
    }

    final Map<String, Node> nodes = new LinkedHashMap<>();

    static Map<String, Supplier<? extends MigratableActor>> factories() {
        Map<String, Supplier<? extends MigratableActor>> f = new HashMap<>();
        f.put(CounterActor.TYPE, CounterActor::new);
        f.put(MisbehavingActors.SLOW, MisbehavingActors.Slow::new);
        f.put(MisbehavingActors.NO_SNAPSHOT, MisbehavingActors.NoSnapshot::new);
        f.put(MisbehavingActors.NO_RESTORE, MisbehavingActors.NoRestore::new);
        return f;
    }

    static LocalCluster of(String... names) throws IOException {
        return of(FAST, Map.of(), names);
    }

    /**
     * @param configs per-node config overrides; nodes not listed use defaultConfig
     */
    static LocalCluster of(MigrationManager.Config defaultConfig, Map<String, MigrationManager.Config> configs,
                          String... names) throws IOException {
        return of(defaultConfig, configs, Map.of(), names);
    }

    /**
     * @param hostFactories per-node actor factories; nodes not listed get factories()
     */
    static LocalCluster of(MigrationManager.Config defaultConfig, Map<String, MigrationManager.Config> configs,
                          Map<String, Map<String, Supplier<? extends MigratableActor>>> hostFactories,
                          String... names) throws IOException {
        LocalCluster cluster = new LocalCluster();
        Map<String, String> addresses = new HashMap<>();
        for (String name : names) {
            TCPTransport transport = new TCPTransport(name, 0);
            transport.start();
            addresses.put(name, "127.0.0.1:" + transport.boundPort());
            LocalActorHost host = new LocalActorHost(hostFactories.getOrDefault(name, factories()));
            cluster.nodes.put(name, new Node(name, transport, host));
        }
        for (Node node : cluster.nodes.values()) {
            Map<String, String> peers = new HashMap<>(addresses);
            peers.remove(node.name);
            node.manager = new MigrationManager(node.host, node.registry, node.faulty, peers,
                configs.getOrDefault(node.name, defaultConfig));
        }
        return cluster;
    }

    Node node(String name) {
        return nodes.get(name);
    }

    /** Asserts count, sum of 1..n, and order for numbered messages 1..n. */
    static void assertInvariants(CounterActor state, long n) {
        assertEquals(n, state.count(), "no loss: every message processed");
        assertEquals(n * (n + 1) / 2, state.sum(), "no duplication: sum of 1..n");
        assertEquals(0, state.outOfOrder(), "per-sender order kept");
    }

    @Override
    public void close() {
        for (Node node : nodes.values()) {
            node.manager.close();
            node.transport.close();
        }
    }
}
