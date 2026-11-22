package com.northeastern.cs5010.bench;

import com.northeastern.cs5010.baselines.CounterService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Runs a workload against a CounterService with real client threads and
 * measures throughput, read latency, and correctness.
 */
public final class BenchmarkRunner {

    private BenchmarkRunner() {
    }

    public static BenchmarkResult run(CounterService service, WorkloadSpec spec) throws InterruptedException {
        WorkloadGenerator generator = new WorkloadGenerator(spec);
        Operation[][] plans = new Operation[spec.clients()][];
        long writes = 0;
        for (int c = 0; c < spec.clients(); c++) {
            plans[c] = generator.opsFor(c);
            for (Operation op : plans[c]) {
                if (op.kind() == Operation.Kind.WRITE) {
                    writes++;
                }
            }
        }

        LatencyRecorder[] latencies = new LatencyRecorder[spec.clients()];
        CountDownLatch start = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread[] clients = new Thread[spec.clients()];
        for (int c = 0; c < spec.clients(); c++) {
            Operation[] plan = plans[c];
            LatencyRecorder recorder = new LatencyRecorder();
            latencies[c] = recorder;
            clients[c] = new Thread(() -> runClient(service, plan, recorder, start, failure), "bench-client-" + c);
            clients[c].start();
        }

        long began = System.nanoTime();
        start.countDown();
        for (Thread client : clients) {
            client.join();
        }
        if (failure.get() != null) {
            throw new IllegalStateException("client failed during benchmark", failure.get());
        }
        long total = service.total();
        long elapsed = System.nanoTime() - began;

        LatencyRecorder all = new LatencyRecorder();
        for (LatencyRecorder r : latencies) {
            all.addAll(r);
        }
        return new BenchmarkResult(service.name(), spec, writes, spec.totalOps() - writes, elapsed, total,
            all.percentileMicros(50), all.percentileMicros(95), all.percentileMicros(99));
    }

    private static void runClient(CounterService service, Operation[] plan, LatencyRecorder recorder,
                                  CountDownLatch start, AtomicReference<Throwable> failure) {
        try {
            start.await();
            for (Operation op : plan) {
                if (op.kind() == Operation.Kind.WRITE) {
                    service.increment(op.key());
                } else {
                    long t0 = System.nanoTime();
                    service.get(op.key());
                    recorder.record(System.nanoTime() - t0);
                }
            }
        } catch (Throwable t) {
            failure.compareAndSet(null, t);
        }
    }
}
