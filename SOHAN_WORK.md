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
