# Sohan's Work: Transport Layer, Location Registry, Pi Deployment

## Phase 2

- `Transport` interface for inter-node messaging
- `LocalTransport` MVP for single-JVM testing
- `LocationRegistry` tracking actor locations with epoch numbers and status (ACTIVE, MIGRATING, PAUSED)

## Phase 3

### Transport layer: TCP/TLS codec (`transport` package)

| Class | Purpose |
|---|---|
| `AsyncTransport` | Transport that can push inbound messages to a listener instead of being polled. |
| `FramedTransport` | Shared codec: 4-byte length prefix, sender name inside each frame, one ordered inbox per sender, frame counters. |
| `TCPTransport` | Non-blocking TCP. One NIO `Selector` thread serves every inbound connection; outbound connections are pooled per destination. |
| `TlsTransport` | Mutual TLS 1.3. Both sides present a certificate signed by the cluster CA, and both verify the other. One virtual thread per connection. |
| `TlsContexts` | Loads a node's PKCS12 keystore (its own key plus the cluster CA) into an `SSLContext`. |

Design choices:
- **One selector thread for TCP.** The Pi 4 has 4 cores, so a thread per connection does not scale there; one selector does.
- **Virtual threads for TLS.** The blocking TLS API is simpler and less error-prone than `SSLEngine` over NIO, and Java 21 virtual threads make one thread per connection cheap again.
- **Length-prefixed frames.** TCP is a byte stream, so messages can split or merge across reads. The decoder reassembles them and rejects corrupt length prefixes.
- **Bounded inboxes.** A full inbox blocks the reader, which pushes back on a fast sender instead of exhausting the Pi's memory.
- **Fault isolation.** A malformed frame, a bad length prefix, or an untrusted peer drops only that one connection; the node keeps serving everyone else.
- **Mutual TLS with a cluster CA.** A device without a CA-signed key cannot send or receive traffic. Each node has its own key, so one node can be replaced without re-keying the others.
- **Clean shutdown.** `close()` closes the server socket, every outbound connection, and every accepted inbound connection, then waits for the I/O thread, so the port and sockets are really released.

Certificates are generated per deployment with `scripts/gen-cluster-certs.sh` and are never committed:

```bash
scripts/gen-cluster-certs.sh certs <password> pi mac
```

### Location registry

`LocationRegistry` now stores immutable entries and changes them only through atomic operations, so two threads can never interleave a read and a write on the same actor:
- `register(actorId, location, epoch)` records an actor at a known epoch.
- `beginMigration(actorId, expectedLocation)` moves ACTIVE to MIGRATING only if the actor is active at the expected location, so two moves of one actor can never both start.
- `commitMigration(actorId, expectedEpoch, newLocation)` moves the actor and bumps the epoch only if the epoch has not changed, so the epoch works as a fencing token.

### Pi deployment

Raspberry Pi 4 Model B running Raspberry Pi OS Lite (64-bit), with OpenJDK 21, Maven, and Git installed. To build and run the full test suite on the Pi:

```bash
git clone https://github.com/sohanmshetty49/EdgeActors.git
cd EdgeActors
mvn clean test
```

### Testing

- Transport and registry tests: `TCPTransportTest`, `FramedTransportTest`, `TlsTransportTest`, `LocalTransportTest`, `LocationRegistryTest`, `LocationRegistryMigrationTest`.
- TLS tests generate throwaway keystores with `keytool` at test time, so no keys are committed. They cover untrusted servers, untrusted clients, plain TCP clients hitting the TLS port, and bad frames from authenticated members.
- Failure paths covered: frames split across reads, corrupt length prefixes, frames without a sender, peers that disconnect, peer restarts with reconnection, unreachable nodes, and oversized messages.
- Registry tests include a race where eight threads try to start the same migration and exactly one wins.

### Per-actor metrics (FR12)

- `ActorSystem.processedCount()` and `averageProcessingMicros()` report how many messages an actor handled and the mean time spent in `receive`, failed messages included.
- `LocalActorHost.metrics(actor)` reports the same for migratable actors, plus mailbox depth, failures and restarts; `mailboxDepths()` lists every actor's depth.
- Node shell: `metrics <actor>` prints the current node, mailbox depth, processed count, mean processing time, failures and restarts.

### Supervision for migrated actors

Migratable actors used to log a failed message and carry on. `LocalActorHost` now applies the same `Supervision` policies as the `ActorSystem`:
- RESTART replaces the instance and keeps the mailbox; each restart uses the actor's restart budget.
- STOP, ESCALATE, or an exhausted budget stops the actor. The host is the top of the tree for migrated actors, so an escalated failure stops the actor, as the root guardian does in the `ActorSystem`.
- Messages queued behind a fatal failure, and any sent afterwards, are counted as dead letters (`deadLetterCount()`, shown in `stats`), never dropped silently. A snapshot waiting behind the failure fails instead of hanging.
- The policy and a fresh restart budget apply on whichever node currently hosts the actor.

### Automatic migration (FR11)

`AutoMigrator` checks the node's load at a fixed interval. When an actor's mailbox reaches the threshold, or system CPU load reaches its threshold, it migrates the actor with the deepest mailbox to the configured node through the normal handoff protocol. A cooldown (10 s) after every attempt prevents bursts of migrations. Each decision is logged.

```bash
java -cp target/edge-actors-0.1.0.jar com.northeastern.cs5010.migration.NodeMain \
    --name pi --port 9100 --peer mac=<mac-ip>:9100 --auto-migrate mac --auto-depth 100 --auto-cpu 0.8
```

Run the trigger on the edge node only; two nodes triggering toward each other could move actors back and forth.

### Migration pause time

- `BlobActor` holds a chosen amount of state (`fill <bytes>`), and the node shell's `pause <node> <bytes> [runs]` times repeated migrations of a fresh blob actor and prints mean, standard deviation, min and max.
- The frame limit was raised from 1 MiB to 8 MiB, because a 1 MB actor is about 1.4 MB once encoded.
- The frame decoder now grows its buffer by doubling instead of copying everything it has buffered on every read. Before, a 1.4 MB frame arriving in 8 KB reads was copied about 175 times.

### Tests added

| Test class | What it checks |
|---|---|
| `ProcessingMetricsTest`, `ActorMetricsTest` | Processed counts and processing time in both runtimes; mailbox depths; the `metrics` command for local, remote and unknown actors |
| `HostSupervisionTest` | Restart keeps later messages; exhausted budget and STOP stop the actor and count dead letters; ESCALATE stops at the node root; sliding budget window; a waiting snapshot fails instead of hanging; a migrated actor is supervised on its new node |
| `AutoMigratorTest` | No trigger when quiet or empty; deep mailbox moves only the busiest actor with no loss; CPU trigger; cooldown; aborted attempts; running on its own under load; flags and validation |
| `LargeFrameTest`, `PauseTimeTest` | Random frames in random read sizes; 3 MB frames; a 2 MB message over real TCP; a 1 MB actor migrating over TCP; the `pause` command |

### Pi evaluation

Run on the Raspberry Pi 4 (edge) and a laptop on the same Wi-Fi (standing in for the cloud node).

**1. Full test suite on the Pi**

```bash
cd ~/EdgeActors && git pull && mvn clean test
```

| Machine | Tests | Result |
|---|---:|---|
| Raspberry Pi 4 | | |

**2. Benchmarks on the Pi** (same command on the laptop for comparison)

```bash
mvn -q compile
java -cp target/classes com.northeastern.cs5010.bench.BenchmarkMain --faults --net-faults 50 2>/dev/null | tee bench-pi.txt
```

| Machine | threads+locks (ops/s) | channels (ops/s) | actors (ops/s) | actors read p99 (us) | Network fault runs: invariants held? |
|---|---:|---:|---:|---:|---|
| Raspberry Pi 4 | | | | | |
| Laptop | | | | | |

**3. Migration pause time, Pi to laptop** (both nodes running `NodeMain`, at the `pi>` prompt)

```
pause mac 1024 10
pause mac 102400 10
pause mac 1048576 10
```

| State size | Mean (ms) | Std dev (ms) | Min (ms) | Max (ms) | Committed |
|---:|---:|---:|---:|---:|---|
| 1 KB | | | | | |
| 100 KB | | | | | |
| 1 MB | | | | | |

**4. Live migration and auto-migration demo**

Manual: `spawn c`, `send c 1000`, `migrate c mac`, `send c 1000`, then `status c` on the laptop should show `count=2000 ... outOfOrder=0`.

Automatic: start the Pi node with `--auto-migrate mac --auto-cpu 0.8`, `spawn c`, then load the Pi's CPU from a second SSH session with `for i in 1 2 3 4; do yes > /dev/null & done`. Within a second the Pi logs `auto-migrating c to mac: CPU load ...`. Stop the load with `kill %1 %2 %3 %4`.

| Demo | Result |
|---|---|
| Manual migration under load, Pi to laptop | |
| Automatic migration on CPU load | |
