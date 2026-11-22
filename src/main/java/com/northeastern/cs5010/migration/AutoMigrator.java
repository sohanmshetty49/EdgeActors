package com.northeastern.cs5010.migration;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;

/**
 * Starts migrations automatically when this node is overloaded (FR11).
 *
 * <p>At a fixed interval it looks at the mailbox depth of every actor hosted
 * here and, optionally, at the system CPU load. When an actor's mailbox
 * reaches the threshold, or the CPU load reaches its threshold, the actor
 * with the deepest mailbox is migrated to the destination node through the
 * normal handoff protocol, so the no loss, no duplication and single location
 * guarantees are the same as for a manual migration.
 *
 * <p>After every attempt the trigger waits for the cooldown before it tries
 * again, so one overloaded moment causes one migration rather than a burst.
 * Run the trigger on the edge node only: if two nodes trigger toward each
 * other, actors could move back and forth.
 *
 * <p>The freeze step finishes the messages already queued before the actor
 * moves, so the mailbox threshold should stay well below what the actor can
 * process within the migration's idle timeout.
 */
public final class AutoMigrator implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(AutoMigrator.class.getName());

    /**
     * When to migrate.
     *
     * @param destination node that receives actors when this node is overloaded
     * @param mailboxThreshold migrate when an actor's mailbox depth reaches this
     * @param cpuThreshold migrate when system CPU load reaches this fraction; 0 turns the CPU check off
     * @param interval how often the load is checked
     * @param cooldown minimum time between two automatic migrations
     */
    public record Policy(String destination, int mailboxThreshold, double cpuThreshold, Duration interval,
                         Duration cooldown) {
        public Policy {
            if (destination == null || destination.isBlank()) {
                throw new IllegalArgumentException("auto-migration needs a destination node");
            }
            if (mailboxThreshold < 1) {
                throw new IllegalArgumentException("mailbox threshold must be at least 1");
            }
            if (!(cpuThreshold >= 0 && cpuThreshold <= 1)) {
                throw new IllegalArgumentException("cpu threshold must be between 0 and 1");
            }
            Objects.requireNonNull(interval, "interval");
            Objects.requireNonNull(cooldown, "cooldown");
            if (interval.isZero() || interval.isNegative()) {
                throw new IllegalArgumentException("interval must be positive");
            }
            if (cooldown.isNegative()) {
                throw new IllegalArgumentException("cooldown must not be negative");
            }
        }

        /** Checks twice a second and waits 10 seconds between migrations. */
        public static Policy of(String destination, int mailboxThreshold, double cpuThreshold) {
            return new Policy(destination, mailboxThreshold, cpuThreshold, Duration.ofMillis(500),
                Duration.ofSeconds(10));
        }

        public String describe() {
            return "to " + destination + " when a mailbox reaches " + mailboxThreshold
                + (cpuThreshold > 0 ? " or CPU load reaches " + Math.round(cpuThreshold * 100) + "%" : "");
        }
    }

    /** One automatic migration: which actor, why, and how it went. */
    public record Decision(String actorId, String reason, MigrationManager.Result result) {
    }

    private final MigrationManager manager;
    private final LocalActorHost host;
    private final Policy policy;
    private final DoubleSupplier cpuLoad;
    private final LongSupplier nanos;
    private final ScheduledExecutorService timer;
    private final AtomicLong triggered = new AtomicLong();
    private final AtomicLong committed = new AtomicLong();
    private long lastAttempt;
    private boolean attempted;
    private boolean started;
    private volatile Decision lastDecision;

    public AutoMigrator(MigrationManager manager, LocalActorHost host, Policy policy) {
        this(manager, host, policy, AutoMigrator::systemCpuLoad, System::nanoTime);
    }

    /** cpuLoad returns a fraction from 0 to 1; nanos is the clock for the cooldown. Tests pass fakes. */
    AutoMigrator(MigrationManager manager, LocalActorHost host, Policy policy, DoubleSupplier cpuLoad,
                 LongSupplier nanos) {
        this.manager = Objects.requireNonNull(manager, "manager");
        this.host = Objects.requireNonNull(host, "host");
        this.policy = Objects.requireNonNull(policy, "policy");
        this.cpuLoad = Objects.requireNonNull(cpuLoad, "cpuLoad");
        this.nanos = Objects.requireNonNull(nanos, "nanos");
        if (policy.destination().equals(manager.nodeName())) {
            throw new IllegalArgumentException("auto-migration destination cannot be this node");
        }
        this.timer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, manager.nodeName() + "-auto-migrate");
            t.setDaemon(true);
            return t;
        });
    }

    /** Starts checking at the policy's interval. Calling it again has no effect. */
    public synchronized void start() {
        if (started) {
            return;
        }
        started = true;
        long every = policy.interval().toMillis();
        timer.scheduleWithFixedDelay(this::checkQuietly, every, every, TimeUnit.MILLISECONDS);
    }

    private void checkQuietly() {
        try {
            check();
        } catch (RuntimeException e) {
            // A failed check must not cancel the schedule.
            LOG.log(System.Logger.Level.WARNING, "auto-migration check failed", e);
        }
    }

    /**
     * Checks the load once and migrates one actor if the node is overloaded.
     * Returns what it did, or empty if nothing needed to move.
     */
    public synchronized Optional<Decision> check() {
        long now = nanos.getAsLong();
        if (attempted && now - lastAttempt < policy.cooldown().toNanos()) {
            return Optional.empty();
        }
        // Sorted by id so ties pick the same actor every time.
        Map<String, Integer> depths = new TreeMap<>(host.mailboxDepths());
        if (depths.isEmpty()) {
            return Optional.empty();
        }
        String busiest = null;
        int deepest = -1;
        for (Map.Entry<String, Integer> e : depths.entrySet()) {
            if (e.getValue() > deepest) {
                busiest = e.getKey();
                deepest = e.getValue();
            }
        }
        String reason;
        if (deepest >= policy.mailboxThreshold()) {
            reason = "mailbox depth " + deepest + " reached " + policy.mailboxThreshold();
        } else if (policy.cpuThreshold() > 0) {
            double load = cpuLoad.getAsDouble();
            if (load < policy.cpuThreshold()) {
                return Optional.empty();
            }
            reason = "CPU load " + Math.round(load * 100) + "% reached " + Math.round(policy.cpuThreshold() * 100) + "%";
        } else {
            return Optional.empty();
        }

        attempted = true;
        lastAttempt = now;
        triggered.incrementAndGet();
        LOG.log(System.Logger.Level.INFO, "auto-migrating " + busiest + " to " + policy.destination() + ": " + reason);
        MigrationManager.Result result = manager.migrate(busiest, policy.destination());
        if (result.committed()) {
            committed.incrementAndGet();
        }
        LOG.log(System.Logger.Level.INFO, "auto-migration of " + busiest + " " + result.outcome()
            + " in " + result.durationMillis() + " ms" + (result.reason().isEmpty() ? "" : " (" + result.reason() + ")"));
        Decision decision = new Decision(busiest, reason, result);
        lastDecision = decision;
        return Optional.of(decision);
    }

    public Policy policy() {
        return policy;
    }

    /** Automatic migrations attempted so far. */
    public long triggeredCount() {
        return triggered.get();
    }

    /** Automatic migrations that committed. */
    public long committedCount() {
        return committed.get();
    }

    /** The most recent automatic migration, or null if none happened yet. */
    public Decision lastDecision() {
        return lastDecision;
    }

    @Override
    public void close() {
        timer.shutdownNow();
    }

    /** System CPU load from 0 to 1, or 0 if the platform does not report it. */
    public static double systemCpuLoad() {
        OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        if (os instanceof com.sun.management.OperatingSystemMXBean sun) {
            double load = sun.getCpuLoad();
            return load < 0 ? 0 : load;
        }
        double perCore = os.getSystemLoadAverage() / os.getAvailableProcessors();
        return perCore < 0 ? 0 : Math.min(1, perCore);
    }
}
