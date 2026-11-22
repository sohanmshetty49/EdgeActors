package com.northeastern.cs5010.baselines;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Baseline 2: message passing over channels, in the style of CSP or Go.
 *
 * <p>Keys are split into shards. Each shard is owned by one dedicated
 * platform thread that reads requests from its channel (a BlockingQueue) and
 * is the only thread that ever touches that shard's counters, so no locks are
 * needed. Unlike the actor runtime there is no supervision, no addressing
 * layer, and no location transparency: just threads and queues.
 */
public final class ChannelCounterService implements CounterService {

    private sealed interface Request permits Inc, Get, Sum, Stop {
    }

    private record Inc(int key) implements Request {
    }

    private record Get(int key, CompletableFuture<Long> reply) implements Request {
    }

    private record Sum(CompletableFuture<Long> reply) implements Request {
    }

    private record Stop() implements Request {
    }

    private final int keys;
    private final List<BlockingQueue<Request>> channels = new ArrayList<>();
    private final List<Thread> owners = new ArrayList<>();
    private volatile boolean closed;

    public ChannelCounterService(int keys, int shards) {
        if (keys < 1 || shards < 1) {
            throw new IllegalArgumentException("keys and shards must be at least 1");
        }
        this.keys = keys;
        for (int s = 0; s < shards; s++) {
            BlockingQueue<Request> channel = new LinkedBlockingQueue<>(1_000);
            Thread owner = new Thread(() -> serve(channel, new long[keys]), "channel-shard-" + s);
            owner.setDaemon(true);
            channels.add(channel);
            owners.add(owner);
            owner.start();
        }
    }

    /** The owner loop: the only code that reads or writes this shard's counters. */
    private static void serve(BlockingQueue<Request> channel, long[] counts) {
        try {
            while (true) {
                Request r = channel.take();
                switch (r) {
                    case Inc inc -> counts[inc.key()]++;
                    case Get get -> get.reply().complete(counts[get.key()]);
                    case Sum sum -> {
                        long total = 0;
                        for (long c : counts) {
                            total += c;
                        }
                        sum.reply().complete(total);
                    }
                    case Stop stop -> {
                        return;
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public String name() {
        return "channels";
    }

    @Override
    public void increment(int key) throws InterruptedException {
        channelFor(key).put(new Inc(key));
    }

    @Override
    public long get(int key) throws InterruptedException {
        CompletableFuture<Long> reply = new CompletableFuture<>();
        channelFor(key).put(new Get(key, reply));
        return await(reply);
    }

    @Override
    public long total() throws InterruptedException {
        List<CompletableFuture<Long>> replies = new ArrayList<>();
        for (BlockingQueue<Request> channel : channels) {
            CompletableFuture<Long> reply = new CompletableFuture<>();
            channel.put(new Sum(reply));
            replies.add(reply);
        }
        long sum = 0;
        for (CompletableFuture<Long> reply : replies) {
            sum += await(reply);
        }
        return sum;
    }

    private BlockingQueue<Request> channelFor(int key) {
        if (closed) {
            throw new IllegalStateException("service is closed");
        }
        if (key < 0 || key >= keys) {
            throw new IllegalArgumentException("key out of range: " + key);
        }
        return channels.get(key % channels.size());
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
        for (BlockingQueue<Request> channel : channels) {
            channel.offer(new Stop());
        }
        for (Thread owner : owners) {
            try {
                owner.join(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
