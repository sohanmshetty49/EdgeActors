# EdgeActors

[![CI](https://github.com/sohanmshetty49/EdgeActors/actions/workflows/ci.yml/badge.svg)](https://github.com/sohanmshetty49/EdgeActors/actions/workflows/ci.yml)

EdgeActors is a Java 21 actor framework that can move a running actor between a Raspberry Pi edge node and a cloud node without losing or duplicating messages. Each actor has private state and a mailbox and is reached only through messages. Actors are arranged in supervision trees that restart, stop, or escalate on failure, and a two phase handoff protocol over TCP or mutual TLS moves an actor's state and pending messages to another node while senders keep using the same address. The project also compares the actor model with threads and locks and with channel based concurrency on the same workload.

## Team

| Member | GitHub Username | Role |
|---|---|---|
| Sohan Mahesh | @sohanmshetty49 | Transport layer (TCP/TLS codec), location registry, Raspberry Pi deployment |
| Anusha Ravi Kumar | @anusha24-bit | Actor core, supervision tree, migration handoff protocol, correctness argument |
| Shravya Kishan Bharadwaj | @shraavya-BK | Threads with locks and channel baselines, workload generator, failure injection harness, benchmarks |

Detailed write ups per member: `SOHAN_WORK.md`, `ANUSHA_WORK.md`, `SHRAAVYA_WORK.md`.

## Repository Structure

The packages follow the components in the Phase 1 SRS (Section 8.1).

```
EdgeActors/
  src/main/java/com/northeastern/cs5010/
    actors/       Actor runtime and supervision: ActorSystem, Actor, ActorRef, Message, Mailbox,
                  ActorRunner, ActorCell, Supervision, RestartBudget, EscalatedFailure
    transport/    Transport and registry: Transport, AsyncTransport, LocalTransport,
                  FramedTransport (frame codec), TCPTransport, TlsTransport, TlsContexts, LocationRegistry
    migration/    Migration manager: MigrationManager (handoff protocol), HandoffMessage, ActorHost,
                  LocalActorHost (supervised host), MigratableActor, CounterActor, BlobActor,
                  AutoMigrator (automatic migration), NodeMain, NodeShell, NodeConfig
    baselines/    Same workload in three models: CounterService, LockedCounterService,
                  ChannelCounterService, ActorCounterService
    bench/        Workload generator and harnesses: WorkloadSpec, WorkloadGenerator, BenchmarkRunner,
                  LatencyRecorder, FaultPlan, FaultInjectionHarness (actor crashes), ChaosTransport,
                  MigrationFaultHarness, SequenceRecorder (network faults), BenchmarkReport, BenchmarkMain
    examples/     EchoActor
  src/test/java/com/northeastern/cs5010/
    actors/  transport/  migration/  baselines/  bench/
  scripts/gen-cluster-certs.sh    creates the cluster CA and one TLS keystore per node
  .github/workflows/ci.yml        builds and tests every push on Java 21
  SOHAN_WORK.md  ANUSHA_WORK.md  SHRAAVYA_WORK.md
  pom.xml
```

## Setup and Run

**Prerequisites:** Java 21 and Maven 3.8+. The edge node is a Raspberry Pi 4 running Raspberry Pi OS Lite (64 bit) with OpenJDK 21, Maven and Git.

```bash
# 1. Clone
git clone https://github.com/sohanmshetty49/EdgeActors.git
cd EdgeActors

# 2. Build and run all tests
mvn clean test

# 3. Coverage report
# open target/site/jacoco/index.html

# 4. Build the jar used by the commands below
mvn -q package -DskipTests
```

### Live migration between two nodes (primary use case UC1)

Start one node on each machine. Replace the example addresses with the real ones.

```bash
# On the Raspberry Pi (other node at 192.168.1.20)
java -cp target/edge-actors-0.1.0.jar com.northeastern.cs5010.migration.NodeMain \
    --name pi --port 9100 --peer mac=192.168.1.20:9100

# On the laptop or cloud node (Pi at 192.168.1.50)
java -cp target/edge-actors-0.1.0.jar com.northeastern.cs5010.migration.NodeMain \
    --name mac --port 9100 --peer pi=192.168.1.50:9100
```

Commands at the `pi>` prompt:

```
spawn c            start counter actor "c" on this node
send c 100         send it 100 numbered messages
migrate c mac      move it to the other node while it runs (prints the outcome and time in ms)
status c           location, epoch, state, and the counter value
metrics c          current node, mailbox depth, processed, mean processing time, failures, restarts
pause mac 1048576  time migrations of an actor with 1 MB of state (mean and spread over runs)
stats              transport and migration counters, memory, cores, dead letters
quit
```

To run the same nodes over mutual TLS, create keystores once and add `--tls` to each node:

```bash
scripts/gen-cluster-certs.sh certs <password> pi mac
# copy certs/pi.p12 to the Pi only; keep certs/mac.p12 on the other node
java -cp target/edge-actors-0.1.0.jar com.northeastern.cs5010.migration.NodeMain \
    --name pi --port 9100 --peer mac=192.168.1.20:9100 --tls certs/pi.p12 --password <password>
```

The `certs/` folder contains private keys and is never committed.

To let the Pi offload actors on its own when it is overloaded (FR11), add to the Pi's command:

```bash
    --auto-migrate mac --auto-depth 100 --auto-cpu 0.8
```

It migrates the busiest actor when a mailbox reaches 100 messages or CPU load reaches 80%, at most once every 10 seconds.

### Benchmarks and failure injection

```bash
mvn -q compile
java -cp target/classes com.northeastern.cs5010.bench.BenchmarkMain --faults --net-faults 50 2>/dev/null
```

This runs the same workload on threads with locks, channels, and actors, prints throughput and read latency (p50, p95, p99), and checks that no update was lost. `--faults` adds actor crash injection under supervision, and `--net-faults N` runs N randomized live migrations per row under message delay, lost protocol messages, and network partitions, checking all three invariants on every run. All options are in `SHRAAVYA_WORK.md`.

## Feature Status

| Functional Requirement (from Phase 1) | Status | Where to Find It |
|---|---|---|
| FR1: Actor with private state and a mailbox, one message at a time | Complete | `actors/Actor`, `actors/Mailbox`, `actors/ActorRunner` |
| FR2: Asynchronous send through an actor reference | Complete | `ActorSystem.send()`, `actors/ActorRef` |
| FR3: Per sender message order | Complete | `actors/Mailbox` (FIFO), `transport/FramedTransport` (one ordered inbox per sender) |
| FR4: Supervision tree with restart, stop, escalate | Complete | `actors/Supervision`, `ActorSystem.handleFailure()`, `ActorSystem.spawnChild()` |
| FR5: Restart budget of N restarts within T seconds | Complete | `actors/RestartBudget`; an exhausted budget escalates |
| FR6: Encode messages and actor snapshots | Complete | `transport/FramedTransport` (length prefixed frames), `migration/HandoffMessage`, `MigratableActor.snapshot()` / `restore()` |
| FR7: Location registry with node, epoch, and status | Complete | `transport/LocationRegistry` (`beginMigration`, `commitMigration`, epoch fencing) |
| FR8: Live migration with the handoff protocol | Complete | `migration/MigrationManager.migrate()` |
| FR9: Buffer messages during migration and deliver in order | Complete | `MigrationManager` (buffer carried in COMMIT, FORWARD for late messages, ordered retried outbox, DRAINED fence) |
| FR10: Abort and restore at the source | Complete | `MigrationManager` (ABORT path, QUERY to resolve an in doubt result) |
| FR11: Automatic migration on a load threshold | Complete | `migration/AutoMigrator` (mailbox depth or CPU load, cooldown); `NodeMain --auto-migrate` |
| FR12: Per actor metrics (mailbox depth, processing time, current node) | Complete | `ActorSystem.processedCount()` / `averageProcessingMicros()`, `LocalActorHost.metrics()`; `metrics <actor>` in the node shell |
| FR13: Workload generator and failure injection harness | Complete | `bench/WorkloadGenerator`; actor crashes: `bench/FaultInjectionHarness`; network delay, lost messages, partitions: `bench/ChaosTransport`, `bench/MigrationFaultHarness` |
| FR14: Same workload on threads with locks and channel baselines | Complete | `baselines/LockedCounterService`, `baselines/ChannelCounterService`, `baselines/ActorCounterService` |
| UC1: Live migration under load | Complete (one machine) | `MigrationManagerTest` runs it over real TCP sockets; the two node run uses `NodeMain` |
| UC2: Supervised failure recovery | Complete | `SupervisionTest`, `FaultInjectionHarnessTest`, `HostSupervisionTest` (migrated actors) |

## Scope and Design Changes Since Phase 1

| Change | Reason |
|---|---|
| Mutual TLS 1.3 with one certificate per node, signed by a cluster CA, instead of a shared key over TLS | Each node proves its own identity, and a node without a certificate from the cluster CA fails the handshake and is refused. No single secret is shared by every node. |
| Handoff redesigned as a two phase exchange (PREPARE / PREPARED, then COMMIT / COMMITTED, with ABORT and QUERY) | Every step is acknowledged, so each crash point has a defined recovery. Buffered messages travel inside COMMIT, and QUERY settles a migration whose outcome is unknown. |
| Each actor runs on its own Java 21 virtual thread instead of a shared dispatcher pool | Virtual threads make one thread per actor cheap, which keeps the runtime simple and lets thousands of idle actors run on the Pi. |
| Handoff messages and snapshots use a fixed text encoding instead of Jackson JSON | The decoder only accepts the known message types, which avoids the risks of general purpose deserialization. Jackson is still declared in `pom.xml` and will be removed. |
| Custom benchmark runner instead of JMH | The comparison needs many client threads, a correctness check of the final totals, and timing that includes queued asynchronous writes. This is a whole system measurement rather than a microbenchmark. |
| Location registry kept in memory instead of persisted to disk | Actor state is in memory by design (persistence is out of scope), so after a restart a saved registry would point to actors that no longer exist. |
| Two additions to the handoff protocol: an ordered, retried outbox per peer, and a DRAINED message that fences a new location until the old one has forwarded everything | Network fault injection showed that forwarded messages could be lost during a partition, and could arrive out of order when an actor moved back toward its sender. Both are fixed, and the fault runs now show no loss and no reordering. |
| Migrated actors are supervised by their host, which acts as the root of the tree on that node | Migratable actors run in `LocalActorHost`, so the host applies the same `Supervision` policies and restart budget as the `ActorSystem`. |
| Frame size limit raised from 1 MiB to 8 MiB | Needed to migrate an actor with 1 MB of state, the largest size in the Phase 1 performance plan. |
| Work split changed from the Phase 1 timeline: Anusha owns supervision and the correctness argument together with the handoff; Shravya owns both baselines, the harness, and benchmarks | Supervision is part of `ActorSystem`, so it stayed with the actor core. The baselines and the harness share the workload code, so one person owns them. |

## Design Patterns and Principles Applied

| Pattern / Principle | Where | Why |
|---|---|---|
| Template Method | `FramedTransport` with `TCPTransport` and `TlsTransport`; `Actor` with `preStart` / `receive` / `postStop` | The frame codec and per sender inboxes are written once; subclasses supply only the socket handling. Actors override hooks and the runtime controls the lifecycle. |
| Strategy | `CounterService` with three implementations; `Supervision` policy per actor | The benchmark runner works against the interface, so all three models run identical code. Each actor's failure handling is chosen by its policy without changing `ActorSystem`. |
| Dependency Injection / Dependency Inversion | `MigrationManager(ActorHost, LocationRegistry, AsyncTransport, ...)`; `ActorSystem(name, clock)` | The migration manager depends on interfaces, so tests run it over real TCP or over a faulty transport. The injected clock lets restart budget tests control time. |
| Static Factory Methods | `HandoffMessage.prepare()`, `commit()`, `forward()`; `Supervision.restart()`, `stop()`, `escalate()` | Each message or policy is built through a named method that fills only the fields that type uses. |
| Decorator | `ChaosTransport` (and the test doubles `FaultyTransport`, `FlakyLink`) wrap a real transport | Delay, lost messages, and partitions are added around an unchanged transport, so the migration code under test is the production code. |
| Observer | `AsyncTransport.setListener()` pushes inbound messages to `MigrationManager` | The transport does not know about migration; the manager subscribes to what arrives. |
| Immutable value objects | Records: `HandoffMessage`, `Supervision`, `NodeConfig`, `WorkloadSpec`; registry entries replaced atomically | Immutable data can be shared between threads safely, and the registry never shows a half updated entry. |
| Sealed types with pattern matching | `ChannelCounterService.Request`, `ActorCounterService.ShardActor` | The compiler checks that every request type is handled. |
| Single Responsibility / information hiding | `NodeConfig` parses arguments, `NodeShell` parses commands, `MigrationManager` runs the protocol; `ActorCell` is package private | Each class has one job, and actor state can only be reached through messages. |

## Testing

- **Unit tests:** JUnit 5, 132 tests for the actor core, supervision and per actor metrics (`ActorSystemTest`, `SupervisionTest`, `ProcessingMetricsTest`), frame codec and registry (`FramedTransportTest`, `LargeFrameTest`, `LocationRegistryTest`, `LocationRegistryMigrationTest`, `LocalTransportTest`), handoff messages, hosts and host supervision (`HandoffMessageTest`, `LocalActorHostTest`, `HostSupervisionTest`, `ActorMetricsTest`, `CounterActorTest`, `NodeConfigTest`, `NodeMainTest`), the three baselines (a shared contract test for each model), and the workload generator, percentiles, and benchmark runner.
- **Integration tests:** 98 tests that run whole workflows over real sockets:
  - `MigrationManagerTest` (24): migration over TCP while messages keep arriving, chained forwarding, unreachable destination (UC1 flow A1), lost replies with abort or in doubt recovery (flow A2), duplicate PREPARE
  - `ReliableForwardingTest` (5): forwards across a down link are retried in order instead of lost; an actor moving back toward its sender gets that sender's messages in order
  - `AutoMigratorTest` (10): automatic migration on mailbox depth and CPU load, cooldown, and no lost messages when it fires under load
  - `NetworkFaultTest` (7): randomized live migrations under delay, lost protocol messages, and partitions, checking no loss, no duplicates, order, and one live copy
  - `PauseTimeTest` (4): a 1 MB actor migrating over TCP and the `pause` command
  - `TCPTransportTest` and `TlsTransportTest` (26): real sockets, reconnects, untrusted peers
  - `NodeShellTest` (9): the node CLI
  - `FaultInjectionHarnessTest` (7): supervised workers under injected crashes with no lost jobs
  - `BenchmarkMainTest` (6): the full benchmark run
- **Coverage:** 96.1% of instructions and 91.8% of branches (JaCoCo). By package: transport 97%, migration 96%, actors 91%, baselines 94%, bench 97%.
- All 230 tests pass with `mvn clean test`, and GitHub Actions runs them on every push.
- **Fault runs:** 50 randomized runs per fault mix (delay, lost protocol messages, partitions) with 0 lost, 0 duplicated, 0 out of order, and never two live copies. The table is in `SHRAAVYA_WORK.md`.

```bash
mvn clean test                                   # all tests and the JaCoCo report
mvn test -Dtest='MigrationManagerTest,ReliableForwardingTest,AutoMigratorTest,NetworkFaultTest,PauseTimeTest,TCPTransportTest,TlsTransportTest,NodeShellTest,FaultInjectionHarnessTest,BenchmarkMainTest'   # integration tests only
mvn test -Dtest='SupervisionTest'                # one suite
```

## Known Issues

- The Raspberry Pi results (benchmarks, migration pause time for 1 KB, 100 KB and 1 MB, and the two node demo) are measured with the commands in `SOHAN_WORK.md` under "Pi evaluation"; the network fault runs so far ran both nodes on one machine.
- The fault injection covers actor crashes, message delay, lost protocol messages, and partitions. A whole node crashing in the middle of a handoff is not injected yet.
- If the old node never sends DRAINED (for example it crashed), the new node releases the messages it holds after the drain timeout (10 s), so order is only guaranteed while the old node is alive.
- While a link is down, forwarded messages wait in memory on the sending node; they are not persisted and are lost if that node stops.
- The location registry is in memory only. Automatic migrations are written to the log, but manual ones are only printed by the CLI.
- `jackson-databind` is declared in `pom.xml` but unused.

## Team Contributions (Phase 3)

| Member | Contributions This Phase | Commits (total) |
|---|---|---|
| Sohan Mahesh | NIO TCP transport with a shared length prefixed frame codec (`FramedTransport`, `TCPTransport`, `AsyncTransport`); mutual TLS 1.3 transport (`TlsTransport`, `TlsContexts`) and the cluster certificate script; atomic location registry with epoch fencing for migration; per actor processing time metrics (FR12); supervision for migrated actors; automatic migration (`AutoMigrator`, FR11); migration pause time tooling (`BlobActor`, `pause`) and larger frames; Raspberry Pi setup and evaluation; tests for all of these; this README | 8 (11) |
| Anusha Ravi Kumar | Supervision policies and restart budget; supervision tree with cascades and lifecycle hooks; two phase migration handoff with buffering, forwarding, abort, and in doubt recovery (`MigrationManager`, `HandoffMessage`, `LocalActorHost`); node CLI (`NodeMain`, `NodeShell`); correctness argument; fix for a message loss race found by the crash harness; fix for lost and reordered forwards found by network fault injection (ordered retried outbox, DRAINED fence); supervision and migration tests | 8 (11) |
| Shravya Kishan Bharadwaj | Threads with locks, channel, and actor implementations of the same workload; seeded workload generator with hot key skew; latency percentiles; deterministic crash injection harness; network fault injection (`ChaosTransport`, `MigrationFaultHarness`, `SequenceRecorder`) that found two protocol bugs; benchmark runner, CLI, and report; GitHub Actions CI; contract, harness, and benchmark tests | 6 (9) |
