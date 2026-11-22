package com.northeastern.cs5010.bench;

import com.northeastern.cs5010.actors.Actor;
import com.northeastern.cs5010.actors.ActorRef;
import com.northeastern.cs5010.actors.ActorSystem;
import com.northeastern.cs5010.actors.Message;
import com.northeastern.cs5010.actors.Supervision;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Injects crashes into supervised worker actors and measures what the
 * runtime does about them: how many jobs still get processed, how fast a
 * crashed worker is serving again, whether any job silently disappears, and
 * when a worker's restart budget runs out and it is stopped.
 *
 * <p>Which jobs crash comes from a FaultPlan, so every run can be replayed.
 */
public final class FaultInjectionHarness {

    private FaultInjectionHarness() {
    }

    /** One unit of work. When crash is set the worker fails while handling it. */
    record Job(long id, int worker, boolean crash, Tracker tracker) {
    }

    /** Shared bookkeeping, written by the workers and read by the harness. */
    static final class Tracker {
        final Set<Long> processed = ConcurrentHashMap.newKeySet();
        final AtomicLong duplicates = new AtomicLong();
        final AtomicLong crashed = new AtomicLong();
        final AtomicLong recoveries = new AtomicLong();
        final AtomicLongArray pendingCrashAt;
        final LatencyRecorder recovery = new LatencyRecorder();

        Tracker(int workers) {
            pendingCrashAt = new AtomicLongArray(workers);
        }

        void crashing(Job job) {
            crashed.incrementAndGet();
            pendingCrashAt.compareAndSet(job.worker(), 0, System.nanoTime());
        }

        void done(Job job) {
            if (!processed.add(job.id())) {
                duplicates.incrementAndGet();
            }
            long crashedAt = pendingCrashAt.getAndSet(job.worker(), 0);
            if (crashedAt != 0) {
                recoveries.incrementAndGet();
                synchronized (recovery) {
                    recovery.record(System.nanoTime() - crashedAt);
                }
            }
        }
    }

    /** A worker that either does the job or fails on it, as the plan says. */
    public static final class Worker extends Actor {
        @Override
        public void receive(Message message) {
            Job job = (Job) message.getPayload();
            if (job.crash()) {
                job.tracker().crashing(job);
                throw new IllegalStateException("injected fault on job " + job.id());
            }
            job.tracker().done(job);
        }
    }

    /**
     * Sends jobs round-robin to workers supervised by the given policy, with
     * crashes chosen by the plan, and waits until every job is accounted for.
     */
    public static FaultResult run(int workers, long jobs, FaultPlan plan, Supervision supervision,
                                  Duration timeout) throws Exception {
        if (workers < 1 || jobs < 0) {
            throw new IllegalArgumentException("need at least 1 worker and a non-negative job count");
        }
        ActorSystem system = new ActorSystem("faults");
        try {
            List<ActorRef> refs = new ArrayList<>();
            for (int w = 0; w < workers; w++) {
                refs.add(system.actorOf(Worker.class, "worker-" + w, supervision));
            }
            Tracker tracker = new Tracker(workers);
            ActorRef harness = new ActorRef("harness", "faults");

            long began = System.nanoTime();
            for (long id = 0; id < jobs; id++) {
                int w = (int) (id % workers);
                Job job = new Job(id, w, plan.crashes(id), tracker);
                system.send(refs.get(w), new Message(harness, refs.get(w), job));
            }
            long deadline = System.nanoTime() + timeout.toNanos();
            while (accounted(tracker, system) < jobs && System.nanoTime() < deadline) {
                Thread.sleep(1);
            }
            long elapsed = System.nanoTime() - began;

            long processed = tracker.processed.size();
            long crashed = tracker.crashed.get();
            long dead = system.deadLetterCount();
            int alive = (int) refs.stream().filter(system::isAlive).count();
            double p50;
            double p99;
            synchronized (tracker.recovery) {
                p50 = tracker.recovery.percentileMicros(50);
                p99 = tracker.recovery.percentileMicros(99);
            }
            return new FaultResult(workers, jobs, plan.crashProbability(), supervision, processed, crashed, dead,
                jobs - processed - crashed - dead, tracker.duplicates.get(), tracker.recoveries.get(), alive,
                system.rootStopCount(), elapsed, p50, p99);
        } finally {
            system.shutdown();
        }
    }

    private static long accounted(Tracker tracker, ActorSystem system) {
        return tracker.processed.size() + tracker.crashed.get() + system.deadLetterCount();
    }
}
