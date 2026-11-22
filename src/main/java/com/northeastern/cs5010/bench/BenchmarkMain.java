package com.northeastern.cs5010.bench;

import com.northeastern.cs5010.actors.Supervision;
import com.northeastern.cs5010.baselines.ActorCounterService;
import com.northeastern.cs5010.baselines.ChannelCounterService;
import com.northeastern.cs5010.baselines.CounterService;
import com.northeastern.cs5010.baselines.LockedCounterService;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Command-line benchmark: runs the same workload against threads+locks,
 * channels, and actors, then optionally a failure-injection sweep, and prints
 * Markdown tables ready for the report. Run it on the Pi and on a laptop with
 * the same arguments to compare edge and cloud-class hardware.
 *
 * <pre>
 * java -cp target/edge-actors-0.1.0.jar com.northeastern.cs5010.bench.BenchmarkMain \
 *      --clients 4 --ops 50000 --keys 1000 --read-ratio 0.1 --faults --csv results.csv
 * </pre>
 */
public final class BenchmarkMain {

    static final String USAGE = "usage: BenchmarkMain [--clients N] [--ops N] [--keys N] [--shards N]"
        + " [--read-ratio R] [--hot KEYS:TRAFFIC] [--seed N] [--warmup N] [--repeat N]"
        + " [--csv FILE] [--faults] [--fault-jobs N]";

    static final List<String> MODELS = List.of("threads+locks", "channels", "actors");
    static final double[] CRASH_RATES = {0, 0.001, 0.01, 0.05};

    private BenchmarkMain() {
    }

    public static void main(String[] args) throws Exception {
        System.exit(run(args, System.out));
    }

    static int run(String[] args, PrintStream out) throws Exception {
        Options options;
        try {
            options = Options.parse(args);
        } catch (IllegalArgumentException e) {
            out.println(e.getMessage());
            out.println(USAGE);
            return 2;
        }
        WorkloadSpec spec = options.spec();
        out.println("Environment: " + BenchmarkReport.environment());
        String skew = spec.hotKeyFraction() >= 1.0 ? "uniform keys"
            : String.format("%.0f%% of traffic on %.0f%% of keys", spec.hotTrafficFraction() * 100,
                spec.hotKeyFraction() * 100);
        out.printf("Workload: %d clients x %d ops, %d keys, %d shards, read ratio %.2f, %s, seed %d%n",
            spec.clients(), spec.opsPerClient(), spec.keys(), options.shards(), spec.readRatio(), skew, spec.seed());
        out.printf("Runs: %d warmup, %d measured per model; the median-throughput run is reported%n%n",
            options.warmup(), options.repeat());

        List<BenchmarkResult> results = new ArrayList<>();
        for (String model : MODELS) {
            List<BenchmarkResult> measured = new ArrayList<>();
            for (int i = 0; i < options.warmup() + options.repeat(); i++) {
                try (CounterService service = create(model, spec.keys(), options.shards())) {
                    BenchmarkResult r = BenchmarkRunner.run(service, spec);
                    if (i >= options.warmup()) {
                        measured.add(r);
                    }
                }
            }
            results.add(median(measured));
        }
        out.println(BenchmarkReport.markdown(results));
        if (options.csv() != null) {
            Files.writeString(options.csv(), BenchmarkReport.csv(results));
            out.println("CSV written to " + options.csv());
        }

        if (options.faults()) {
            List<FaultResult> faults = new ArrayList<>();
            Supervision generous = Supervision.restart(1_000_000, Duration.ofMinutes(1));
            for (double rate : CRASH_RATES) {
                faults.add(FaultInjectionHarness.run(options.shards(), options.faultJobs(),
                    new FaultPlan(rate, spec.seed()), generous, Duration.ofSeconds(60)));
            }
            // A tight budget shows the other side: workers that keep failing are stopped.
            faults.add(FaultInjectionHarness.run(options.shards(), options.faultJobs(),
                new FaultPlan(0.05, spec.seed()), Supervision.restart(3, Duration.ofMinutes(1)), Duration.ofSeconds(60)));
            out.println();
            out.println("Failure injection (" + options.shards() + " supervised workers):");
            out.println(BenchmarkReport.faultMarkdown(faults));
        }
        return 0;
    }

    static CounterService create(String model, int keys, int shards) {
        return switch (model) {
            case "threads+locks" -> new LockedCounterService(keys, shards);
            case "channels" -> new ChannelCounterService(keys, shards);
            case "actors" -> new ActorCounterService(keys, shards);
            default -> throw new IllegalArgumentException("unknown model " + model);
        };
    }

    static BenchmarkResult median(List<BenchmarkResult> runs) {
        List<BenchmarkResult> sorted = new ArrayList<>(runs);
        sorted.sort(Comparator.comparingDouble(BenchmarkResult::throughputOpsPerSec));
        return sorted.get(sorted.size() / 2);
    }

    record Options(WorkloadSpec spec, int shards, int warmup, int repeat, Path csv, boolean faults,
                   long faultJobs) {

        static Options parse(String[] args) {
            int clients = 4;
            int ops = 50_000;
            int keys = 1_000;
            int shards = Runtime.getRuntime().availableProcessors();
            double readRatio = 0.1;
            double hotKeys = 1.0;
            double hotTraffic = 1.0;
            long seed = 42;
            int warmup = 1;
            int repeat = 3;
            Path csv = null;
            boolean faults = false;
            long faultJobs = 100_000;
            for (int i = 0; i < args.length; i++) {
                String flag = args[i];
                switch (flag) {
                    case "--clients" -> clients = Integer.parseInt(value(args, ++i, flag));
                    case "--ops" -> ops = Integer.parseInt(value(args, ++i, flag));
                    case "--keys" -> keys = Integer.parseInt(value(args, ++i, flag));
                    case "--shards" -> shards = Integer.parseInt(value(args, ++i, flag));
                    case "--read-ratio" -> readRatio = Double.parseDouble(value(args, ++i, flag));
                    case "--hot" -> {
                        String[] parts = value(args, ++i, flag).split(":");
                        if (parts.length != 2) {
                            throw new IllegalArgumentException("--hot must look like 0.2:0.8");
                        }
                        hotKeys = Double.parseDouble(parts[0]);
                        hotTraffic = Double.parseDouble(parts[1]);
                    }
                    case "--seed" -> seed = Long.parseLong(value(args, ++i, flag));
                    case "--warmup" -> warmup = Integer.parseInt(value(args, ++i, flag));
                    case "--repeat" -> repeat = Integer.parseInt(value(args, ++i, flag));
                    case "--csv" -> csv = Path.of(value(args, ++i, flag));
                    case "--faults" -> faults = true;
                    case "--fault-jobs" -> faultJobs = Long.parseLong(value(args, ++i, flag));
                    default -> throw new IllegalArgumentException("unknown option " + flag);
                }
            }
            if (shards < 1 || warmup < 0 || repeat < 1 || faultJobs < 0) {
                throw new IllegalArgumentException("shards and repeat must be at least 1; warmup and fault jobs at least 0");
            }
            WorkloadSpec spec = new WorkloadSpec(clients, ops, keys, readRatio, hotKeys, hotTraffic, seed);
            return new Options(spec, shards, warmup, repeat, csv, faults, faultJobs);
        }

        private static String value(String[] args, int index, String flag) {
            if (index >= args.length) {
                throw new IllegalArgumentException(flag + " needs a value");
            }
            return args[index];
        }
    }
}
