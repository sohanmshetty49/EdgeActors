# EdgeActors

A Java actor framework with live migration across Raspberry Pi edge nodes and cloud servers. The system enables running actors to seamlessly move between constrained edge devices and powerful cloud infrastructure without losing messages or duplicating work.

## Team

| Member | GitHub Username | Role | Commits |
|---|---|---|---|
| Sohan Mahesh | @sohanmshetty49 | Transport layer (TCP/TLS codec), Location registry, Pi deployment | 3 |
| Anusha Ravi Kumar | @anusha24-bit | Actor core, supervision tree, handoff protocol | 3 |
| Shravya Kishan Bharadwaj | @shraavya-BK | Baseline implementations, workload generator, test harness | 3 |

## Repository Structure

The layout follows the actor model architecture from Phase 1. Core concerns are separated into independent packages: the actor runtime, transport/networking layer, and evaluation baselines.

```
edge-actors/
  src/
    main/java/com/northeastern/cs5010/
      actors/              ActorSystem, Actor base class, Mailbox, Message, ActorRef
      transport/           Transport interface, LocalTransport, LocationRegistry
      examples/            EchoActor (example for end-to-end testing)
    test/java/com/northeastern/cs5010/
      actors/              ActorSystemTest
      transport/           LocationRegistryTest
  pom.xml                  Maven build configuration
  .gitignore
  README.md
```

## Setup and Run

**Prerequisites:** Java 21, Maven 3.8+

```bash
# 1. Clone the repository
git clone https://github.com/sohanmshetty49/EdgeActors.git
cd EdgeActors

# 2. Build the project
mvn clean install

# 3. Run tests
mvn test

# 4. View test coverage
# Coverage reports in target/site/jacoco/index.html
```

**Environment:** No external dependencies required for Phase 2. LocalTransport runs on a single JVM for testing.

## Progress So Far

| Functional Requirement | Status | Notes |
|---|---|---|
| FR1: Private state and mailbox | Complete | ActorRef, Actor, Mailbox with BlockingQueue |
| FR2: Async message send | Complete | ActorSystem.send() non-blocking |
| FR3: Per-sender order | Partial | Mailbox maintains order via timestamps |
| FR4: Supervision tree | Not started | Planned for Phase 3 |
| FR5: Restart budget | Not started | Planned for Phase 3 |
| FR6: Message encoding | Partial | JSON serialization ready; codec pending |
| FR7: Location registry | Complete | LocationRegistry with epoch tracking |
| FR8: Live migration handoff | Not started | Planned for Phase 3 |
| FR9: Message buffering | Not started | Planned for Phase 3 |
| FR10: Abort and restore | Not started | Planned for Phase 3 |
| FR11: Auto-trigger | Not started | Planned for Phase 3 |
| FR12: Per-actor metrics | Partial | Mailbox.depth() implemented |
| FR13: Workload generator | Partial | Test cases written; full harness Phase 3 |
| FR14: Thread/channel baselines | Not started | Planned for Phase 3 |

**Working end to end:** An actor is created on a local ActorSystem, receives messages via its mailbox, and processes them sequentially. The LocationRegistry correctly tracks the actor's location and epoch. Multiple messages are delivered in order and all are processed without loss or duplication.

**Key invariants verified:**
- I1 (No loss): Messages delivered to mailbox are processed or logged
- I2 (No duplication): Each message processed exactly once
- I3 (Single live location): Only one ActorRef location active at a time

## Testing So Far

- **Actor core:** 4 JUnit 5 tests covering actor creation, message send, multiple messages, mailbox depth
- **Transport/Registry:** 6 tests covering registration, location updates, epoch tracking, status changes
- **Coverage:** Initial 60%+ on core packages; improving with Phase 3 additions

All tests pass with `mvn test`.

## Next Steps (Phase 3)

- Implement supervision tree with restart, stop, and escalate policies
- Add restart budget enforcement and cascade logic
- Build TCP/TLS transport codec for inter-node communication
- Implement handoff protocol with message buffering during migration
- Add abort and restore logic for failed migrations
- Build workload generator and failure injection harness
- Implement baseline comparisons (threads with locks, Java channels)
- Run benchmarks on Raspberry Pi and cloud node

## Team Contributions

| Member | Contributions This Phase | Commits |
|---|---|---|
| Sohan Mahesh | Transport interface and LocalTransport MVP, LocationRegistry with epoch tracking and status | 3 |
| Anusha Ravi Kumar | Actor base class, ActorRef, Message envelope, Mailbox with BlockingQueue, ActorSystem, ActorRunner | 3 |
| Shravya Kishan Bharadwaj | Maven configuration (JUnit, JaCoCo), test suite structure, EchoActor example, .gitignore | 3 |
