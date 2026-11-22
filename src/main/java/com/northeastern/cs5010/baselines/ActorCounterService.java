package com.northeastern.cs5010.baselines;

import com.northeastern.cs5010.actors.Actor;
import com.northeastern.cs5010.actors.ActorRef;
import com.northeastern.cs5010.actors.ActorSystem;
import com.northeastern.cs5010.actors.Message;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * The system under test: the same counters implemented with EdgeActors.
 *
 * <p>Each shard is a supervised actor with private state, reached only by
 * messages through the ActorSystem. Compared with the channel baseline this
 * adds what the framework provides (addressing by ActorRef, supervision,
 * virtual-thread mailboxes), so the benchmark shows what those cost.
 */
public final class ActorCounterService implements CounterService {

    /** Messages understood by a shard actor. */
    record Inc(int key) {
    }

    record Get(int key, CompletableFuture<Long> reply) {
    }

    record Sum(CompletableFuture<Long> reply) {
    }

    /** One shard: private counters, touched only from inside receive. */
    public static final class ShardActor extends Actor {
        private final Map<Integer, Long> counts = new HashMap<>();

        @Override
        public void receive(Message message) {
            switch (message.getPayload()) {
                case Inc inc -> counts.merge(inc.key(), 1L, Long::sum);
                case Get get -> get.reply().complete(counts.getOrDefault(get.key(), 0L));
                case Sum sum -> sum.reply().complete(counts.values().stream().mapToLong(Long::longValue).sum());
                default -> throw new IllegalArgumentException("unknown request " + message.getPayload());
            }
        }
    }

    private final int keys;
    private final ActorSystem system = new ActorSystem("bench");
    private final List<ActorRef> shards = new ArrayList<>();
    private final ActorRef client = new ActorRef("bench-client", "bench");
    private volatile boolean closed;

    public ActorCounterService(int keys, int shardCount) {
        if (keys < 1 || shardCount < 1) {
            throw new IllegalArgumentException("keys and shards must be at least 1");
        }
        this.keys = keys;
        try {
            for (int s = 0; s < shardCount; s++) {
                shards.add(system.actorOf(ShardActor.class, "shard-" + s));
            }
        } catch (Exception e) {
            system.shutdown();
            throw new IllegalStateException("could not start shard actors", e);
        }
    }

    @Override
    public String name() {
        return "actors";
    }

    @Override
    public void increment(int key) throws InterruptedException {
        ActorRef shard = shardFor(key);
        system.send(shard, new Message(client, shard, new Inc(key)));
    }

    @Override
    public long get(int key) throws InterruptedException {
        ActorRef shard = shardFor(key);
        CompletableFuture<Long> reply = new CompletableFuture<>();
        system.send(shard, new Message(client, shard, new Get(key, reply)));
        return await(reply);
    }

    @Override
    public long total() throws InterruptedException {
        List<CompletableFuture<Long>> replies = new ArrayList<>();
        for (ActorRef shard : shards) {
            CompletableFuture<Long> reply = new CompletableFuture<>();
            system.send(shard, new Message(client, shard, new Sum(reply)));
            replies.add(reply);
        }
        long sum = 0;
        for (CompletableFuture<Long> reply : replies) {
            sum += await(reply);
        }
        return sum;
    }

    private ActorRef shardFor(int key) {
        if (closed) {
            throw new IllegalStateException("service is closed");
        }
        if (key < 0 || key >= keys) {
            throw new IllegalArgumentException("key out of range: " + key);
        }
        return shards.get(key % shards.size());
    }

    private static long await(CompletableFuture<Long> reply) throws InterruptedException {
        try {
            return reply.get();
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        system.shutdown();
    }
}
