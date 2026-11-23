package com.northeastern.cs5010.bench;

import java.util.List;
import java.util.Locale;

/** Formats results as Markdown tables for the report and CSV for spreadsheets. */
public final class BenchmarkReport {

    private BenchmarkReport() {
    }

    public static String environment() {
        Runtime rt = Runtime.getRuntime();
        return String.format(Locale.ROOT, "%s %s, %d cores, Java %s, max heap %d MB",
            System.getProperty("os.name"), System.getProperty("os.arch"), rt.availableProcessors(),
            System.getProperty("java.version"), rt.maxMemory() / (1024 * 1024));
    }

    public static String markdown(List<BenchmarkResult> results) {
        StringBuilder out = new StringBuilder()
            .append("| Model | Ops | Time (ms) | Throughput (ops/s) | Read p50 (us) | Read p95 (us) | Read p99 (us) | Correct |\n")
            .append("|---|---:|---:|---:|---:|---:|---:|---|\n");
        for (BenchmarkResult r : results) {
            out.append(String.format(Locale.ROOT, "| %s | %d | %.1f | %.0f | %.1f | %.1f | %.1f | %s |%n",
                r.model(), r.operations(), r.elapsedNanos() / 1e6, r.throughputOpsPerSec(),
                r.readP50Micros(), r.readP95Micros(), r.readP99Micros(), r.correct() ? "yes" : "NO"));
        }
        return out.toString();
    }

    public static String csv(List<BenchmarkResult> results) {
        StringBuilder out = new StringBuilder(
            "model,clients,ops_per_client,keys,read_ratio,operations,elapsed_ms,throughput_ops_s,read_p50_us,read_p95_us,read_p99_us,correct\n");
        for (BenchmarkResult r : results) {
            WorkloadSpec s = r.spec();
            out.append(String.format(Locale.ROOT, "%s,%d,%d,%d,%.2f,%d,%.1f,%.0f,%.1f,%.1f,%.1f,%s%n",
                r.model(), s.clients(), s.opsPerClient(), s.keys(), s.readRatio(), r.operations(),
                r.elapsedNanos() / 1e6, r.throughputOpsPerSec(), r.readP50Micros(), r.readP95Micros(),
                r.readP99Micros(), r.correct()));
        }
        return out.toString();
    }

    public static String faultMarkdown(List<FaultResult> results) {
        StringBuilder out = new StringBuilder()
            .append("| Crash rate | Policy | Jobs | Processed | Crashed | Dead letters | Lost | Duplicates | Recoveries | Workers alive | Recovery p50 (us) | Recovery p99 (us) |\n")
            .append("|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        for (FaultResult r : results) {
            out.append(String.format(Locale.ROOT, "| %.3f | %s | %d | %d | %d | %d | %d | %d | %d | %d/%d | %.1f | %.1f |%n",
                r.crashProbability(), describe(r), r.jobs(), r.processed(), r.crashed(), r.deadLetters(),
                r.lost(), r.duplicates(), r.recoveries(), r.workersAlive(), r.workers(),
                r.recoveryP50Micros(), r.recoveryP99Micros()));
        }
        return out.toString();
    }

    /** One row per network fault mix: what was injected, what the migrations did, and the invariant checks. */
    public static String netFaultMarkdown(List<MigrationFaultResult> results) {
        StringBuilder out = new StringBuilder()
            .append("| Faults injected | Runs | Sent | Accepted | Reported to sender | Lost | Duplicates | Out of order | Two live copies | Committed | Aborted | In doubt, resolved | Unresolved | Partitions | Protocol msgs lost | Sends refused by partition |\n")
            .append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        for (MigrationFaultResult r : results) {
            out.append(String.format(Locale.ROOT, "| %s | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d |%n",
                describe(r.settings()), r.settings().runs(), r.sent(), r.accepted(), r.reported(), r.lost(),
                r.duplicates() + r.phantoms(), r.outOfOrder(), r.locationViolations(), r.committed(), r.aborted(),
                r.inDoubtResolved(), r.unresolved(), r.partitions(), r.droppedProtocol(), r.refusedSends()));
        }
        return out.toString();
    }

    static String describe(MigrationFaultHarness.Settings s) {
        StringBuilder d = new StringBuilder("delay up to ").append(s.maxDelayMillis()).append(" ms");
        if (s.dropRate() > 0) {
            d.append(String.format(Locale.ROOT, ", %.0f%% of protocol messages lost", s.dropRate() * 100));
        }
        if (s.partitionChance() > 0) {
            d.append(String.format(Locale.ROOT, ", partition in %.0f%% of runs", s.partitionChance() * 100));
        }
        return d.toString();
    }

    private static String describe(FaultResult r) {
        return switch (r.supervision().directive()) {
            case RESTART -> "restart, " + r.supervision().maxRestarts() + " per " + r.supervision().window().toSeconds() + " s";
            case STOP -> "stop";
            case ESCALATE -> "escalate";
        };
    }
}
