# Shraavya's Work: Testing & Baselines

## Phase 2

### Build & Test Infrastructure
- Maven configuration (pom.xml)
- JUnit 5 test suite
- JaCoCo code coverage reporting
- Example actors for end-to-end testing
- .gitignore and project structure

## Phase 3

Phase 3 compares EdgeActors with two conventional concurrency models on the
same workload, and measures how supervision behaves when actors crash.

### Baselines (`com.northeastern.cs5010.baselines`)

All three models implement one interface, `CounterService` (increment a key,
read a key, read the total), so the benchmark runs identical code against each.

| Model | Class | How it works |
|---|---|---|
| threads+locks | `LockedCounterService` | Shared counters protected by striped `ReentrantLock`s. `total()` takes every stripe in a fixed order, so it sees a consistent snapshot and cannot deadlock. |
| channels | `ChannelCounterService` | CSP / Go style. Each shard is owned by one thread that reads requests from a bounded `BlockingQueue`; only that thread touches the shard's counters, so no locks. Reads get their answer through a reply future. |
| actors | `ActorCounterService` | Each shard is a supervised EdgeActors actor (`ShardActor`) reached only through `ActorSystem.send`. Adds what the framework gives: addressing by `ActorRef`, supervision, and virtual-thread mailboxes. |

### Workload generator (`com.northeastern.cs5010.bench`)

- `WorkloadSpec`: clients, operations per client, key count, read ratio,
  hot-spot skew (for example 80% of traffic on 20% of keys), and a seed.
- `WorkloadGenerator`: builds each client's operation list up front from a
  seeded `SplittableRandom`, so every model gets exactly the same operations
  and generation time is not part of the measurement.
- `BenchmarkRunner`: starts all client threads together behind a latch, times
  from the start until `total()` returns (so queued asynchronous writes are
  counted), records read latency, and checks the final total equals the number
  of writes, catching lost or duplicated updates.
- `LatencyRecorder`: nearest-rank p50 / p95 / p99.

### Failure injection harness

- `FaultPlan`: decides deterministically from a seed (SplitMix64) which job IDs
  crash, so a run can be repeated exactly.
- `FaultInjectionHarness`: sends jobs round-robin to supervised `Worker`
  actors; planned jobs throw. It then accounts for every job:
  `lost = jobs - processed - crashed - dead letters`, which must be 0.
  It also counts duplicates, workers still alive, and recovery time (from a
  crash to that worker's next successful job; back-to-back crashes on the same
  worker count as one outage).
- Writing this harness found a real bug: a message sent at the same moment an
  actor stopped could disappear without being counted. The fix is in
  `ActorSystem.send` / `ActorCell.process` (Anusha's code, committed by her).

### Benchmark CLI and report

`BenchmarkMain` runs all three models (warm-up runs, then repeated measured
runs; the median-throughput run is reported) and prints a Markdown table, an
optional CSV file, and with `--faults` a failure-injection table at crash rates
0, 0.1%, 1% and 5% with a generous restart budget, plus one row with a tight
budget (3 restarts per minute) to show the budget stopping a worker.

```
mvn -q compile
java -cp target/classes com.northeastern.cs5010.bench.BenchmarkMain --faults 2>/dev/null
java -cp target/classes com.northeastern.cs5010.bench.BenchmarkMain \
    --clients 4 --ops 50000 --keys 1000 --shards 4 --read-ratio 0.1 \
    --hot 0.2:0.8 --seed 42 --warmup 1 --repeat 5 --csv results.csv --faults
```

`2>/dev/null` hides the expected warnings the actor system logs when the
tight-budget row stops its workers.

### Example output (2-core cloud VM, sanity check, not the final numbers)

| Model | Ops | Throughput (ops/s) | Read p50 (us) | Read p99 (us) | Correct |
|---|---:|---:|---:|---:|---|
| threads+locks | 200000 | ~13,000,000 | 0.1 | 0.9 | yes |
| channels | 200000 | ~830,000 | 28.2 | 123.2 | yes |
| actors | 200000 | ~860,000 | 22.0 | 144.0 | yes |

Failure injection: at every crash rate with the generous budget, 0 lost,
0 duplicates and all workers alive; only the planned jobs fail. With the tight
budget both workers stop and the remaining jobs are counted as dead letters,
still 0 lost.

Reading the numbers: threads+locks is far faster for this tiny operation
because an increment is a few nanoseconds and the lock is rarely contended;
both message-passing models pay for a queue hand-off per operation. Actors
cost about the same as raw channels while adding supervision and addressing.

### Results on the Raspberry Pi and cloud node

The Pi and laptop runs of these benchmarks are recorded in `SOHAN_WORK.md`
under "Pi evaluation".

### Network fault injection

The crash harness above covers actor failures. These classes add the
network faults from the Phase 1 plan: message delay, lost protocol messages,
and network partitions, injected into real live migrations.

- `ChaosTransport` wraps a real transport. **Delay:** each send waits a
  random time first, in the sending thread, so messages to one node stay in
  order as over TCP. **Lost protocol messages:** PREPARE, PREPARED, REJECT,
  COMMIT, COMMITTED, ABORT and QUERY are dropped with a given probability;
  FORWARD and DRAINED belong to the ordered data stream, which TCP does not
  lose on a working link. **Partition:** while the shared `Network` is cut,
  every send fails with an IOException in both directions.
- `SequenceRecorder` is a migratable actor that records exactly which
  numbered messages arrived, so loss, duplication and reordering can be told
  apart, and its record moves with it.
- `MigrationFaultHarness` runs randomized live migrations: two real nodes
  ("pi" and "cloud") over TCP on one machine, a client streaming numbered
  messages through the Pi, a migration to the cloud at a random moment
  (sometimes back again), random delays, lost replies, and a partition at a
  random time for a random length. Migrations left IN_DOUBT are resolved after
  the network heals. A monitor thread samples both nodes throughout.
- Checked on every run: **I1** every accepted message arrived (a send that
  failed with an error was reported to the client and is not lost); **I2** no
  message arrived twice, and none reported as failed arrived anyway;
  **order** numbers arrived in increasing order; **I3** never two active
  copies, and exactly one at the end; and no migration left undecided.

```
java -cp target/classes com.northeastern.cs5010.bench.BenchmarkMain --net-faults 200 2>/dev/null
```

The first runs found two real protocol bugs: messages lost when a forward hit
a partitioned link, and reordering when an actor moved back toward its
sender. Both were fixed in `MigrationManager` (see `ANUSHA_WORK.md`). After the
fix, 50 randomized runs per row on a 2-core cloud VM:

| Faults injected | Runs | Sent | Accepted | Reported to sender | Lost | Duplicates | Out of order | Two live copies | Committed | Aborted | In doubt, resolved | Unresolved | Partitions | Protocol msgs lost | Sends refused by partition |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| delay up to 3 ms | 50 | 15000 | 15000 | 0 | 0 | 0 | 0 | 0 | 75 | 0 | 0 | 0 | 0 | 0 | 0 |
| delay up to 2 ms, 10% of protocol messages lost | 50 | 15000 | 15000 | 0 | 0 | 0 | 0 | 0 | 62 | 13 | 1 | 0 | 0 | 31 | 0 |
| delay up to 2 ms, 5% of protocol messages lost, partition in 100% of runs | 50 | 15000 | 12957 | 2043 | 0 | 0 | 0 | 0 | 34 | 41 | 1 | 0 | 50 | 10 | 2329 |

Every row: 0 lost, 0 duplicates, 0 out of order, never two live copies,
and every migration decided. "Reported to sender" are sends made during a
partition, which failed with an error the client saw.

### Continuous integration

`.github/workflows/ci.yml` runs `mvn -B clean verify` on Java 21 (Temurin) for
every push to `main` and every pull request, and keeps the JaCoCo coverage
report as a build artifact (plus the Surefire reports when a test fails).

### Tests

| Test class | What it checks |
|---|---|
| `CounterServiceContract` (+ one subclass per model) | Same contract for all three: counts, no lost/duplicated updates with 8 threads x 5000 increments, key range, bad config, idempotent close; channel and actor models also reject use after close and give read-your-writes |
| `WorkloadGeneratorTest` | Same seed gives the same ops, clients differ, read ratio and hot-spot skew within 1%, validation |
| `LatencyRecorderTest` | Nearest-rank percentiles, merging, empty and invalid input |
| `BenchmarkRunnerTest` | Every model processes the workload correctly with the same writes; failures surface; wrong totals flagged |
| `FaultInjectionHarnessTest` | No faults; recovery from 2% crashes with 0 lost; exhausted budget stops workers with 0 lost; stop policy; plan determinism and rate |
| `BenchmarkMainTest` | Full CLI run with CSV and fault table, defaults, bad arguments return exit code 2, report formatting |
| `NetworkFaultTest` | Partitions cut both directions until healed; only requests and replies are dropped; delays keep order; the recorder detects loss, duplicates and reordering; all invariants hold under delay, lost replies and partitions; the `--net-faults` table |

51 tests; about 97% instruction and 90% branch coverage for the `baselines`
and `bench` packages. Stable over repeated runs, including runs pinned to a
single CPU core.
