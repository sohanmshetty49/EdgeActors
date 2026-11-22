# Anusha's Work: Actor Core, Supervision Tree, Handoff Protocol

## Phase 2

### Actor Core Foundation
- Actor base class with receive() lifecycle
- ActorRef: immutable actor reference with UUID
- ActorSystem: manages actor lifecycle and routing
- ActorRunner: polls mailbox and dispatches messages
- Mailbox: BlockingQueue-based async message delivery
- Message: envelope with sender, recipient, payload, timestamp

## Phase 3

### Supervision tree (`actors` package)

| Class | Purpose |
|---|---|
| `Supervision` | A child's policy: RESTART (with a restart budget), STOP, or ESCALATE. |
| `RestartBudget` | Sliding window: at most N restarts within any window of length W. |
| `EscalatedFailure` | The failure a parent receives when a child escalates; names the child, keeps its error as the cause. |
| `ActorCell` | Per-actor record: current instance, mailbox, parent, children, budget, counters. |
| `ActorSystem` | Spawns top-level actors and children, applies supervision, stops subtrees, tracks dead letters. |
| `ActorRunner` | Runs one actor's message loop and reports failures to the system. |
| `Actor` | Adds `preStart()` and `postStop()` lifecycle hooks and access to the system for spawning children. |

**Directives**

| Directive | What happens when the actor's `receive` throws |
|---|---|
| RESTART | The instance is replaced by a fresh one (`postStop` on the old, `preStart` on the new). The mailbox and ref are kept, so only the failing message is lost and everything queued behind it is still processed. Restarting an actor restarts its whole subtree. |
| STOP | The actor and its whole subtree stop. Mail sent afterwards is counted as dead letters. |
| ESCALATE | The parent fails with an `EscalatedFailure`, and the parent's own policy decides. This cascades upward. A failure that reaches the top is handled by the root guardian, which stops that top-level actor. |

**Restart budget and cascade.** A RESTART policy carries a budget such as "3 restarts per minute". When a failure would exceed it, the actor is not restarted again; the failure escalates instead. A crash loop therefore climbs the tree until some supervisor either absorbs it with its own budget or the root guardian stops it. A fresh instance that fails in `preStart` counts against the same budget, so even a broken constructor-time setup terminates.

**API**

```java
ActorRef parent = system.actorOf(Worker.class, "parent", Supervision.restart(5, Duration.ofMinutes(1)));
ActorRef child  = system.spawnChild(parent, Worker.class, "child", Supervision.escalate());
system.stop(parent);                // stops the whole subtree
system.restartCount(child); system.failureCount(child); system.lastFailure(parent);
system.children(parent); system.isAlive(child); system.deadLetterCount(); system.rootStopCount();
```

**Fix to the Phase 2 core.** `ActorSystem` used a fixed pool of 10 threads, and each actor's runner holds its thread for the actor's whole life, so an 11th actor would never run. Actors now run on Java 21 virtual threads, one per actor. A test starts 50 actors and checks they all process messages.

### Live migration handoff protocol (`migration` package)

| Class | Purpose |
|---|---|
| `MigrationManager` | The handoff protocol, one per node. |
| `HandoffMessage` | Protocol messages and their versioned binary codec. |
| `ActorHost` | What the protocol needs from the runtime that runs actors. |
| `LocalActorHost` | Runs migratable actors, one virtual-thread mailbox per actor. |
| `MigratableActor` | An actor whose state can be captured and rebuilt (`snapshot` / `restore`). |
| `CounterActor` | Example actor whose state proves the invariants (count, sum, out-of-order count). |
| `NodeMain`, `NodeConfig`, `NodeShell` | Command-line node that drives migrations between two machines. |

The protocol runs over the transport layer's `AsyncTransport`, so it works unchanged over plain TCP and mutual TLS.

```
source                                  destination
  freeze: ACTIVE -> MIGRATING
  (new messages are buffered)
  wait until actor idle, snapshot
  PREPARE(epoch, state)        ----->   check it can host the actor,
                                        hold the state under a lease
                               <-----   PREPARED | REJECT
  COMMIT(epoch, buffered msgs) ----->   install actor, deliver the batch in order,
                                        register at epoch + 1
                               <-----   COMMITTED | REJECT
  registry -> destination, epoch + 1
  stop local copy, forward anything
  that arrived meanwhile
```

Every request carries a `requestId` and every reply echoes it, so a late reply to an earlier attempt is never mistaken for the answer to the current one.

**Abort and recovery**

| Failure | Outcome |
|---|---|
| Destination rejects PREPARE (no factory, already hosts the actor, another handoff in progress) | ABORTED; the actor keeps running at the source with every buffered message |
| PREPARE or PREPARED lost, destination unreachable | ABORTED after the reply timeout; ABORT tells the destination to drop held state |
| Actor busy past the freeze timeout, or snapshot fails | ABORTED before the destination is contacted |
| COMMIT arrives after the destination's lease expired, or install fails | REJECT; the source restores the actor locally |
| No reply to COMMIT after all retries | IN_DOUBT: the actor is paused and its messages held. `resolve` sends QUERY; the destination answers COMMITTED (the source finishes the move) or REJECT (the destination discards held state so a late COMMIT can never apply, and the source restores the actor) |

## Correctness argument

### Handoff invariants

**I1, no loss.** A message sent to a migrating actor goes to exactly one of three places: the old instance (before the freeze), the buffer (after the freeze), or forwarding (after the move). The freeze and the "is it migrating?" check happen under the same per-actor lock as delivery, so no message can slip between them. Everything in the buffer when COMMIT is sent travels inside COMMIT; anything that arrives later is forwarded after COMMITTED, under the same lock, before any newer message can be routed. On every abort path the buffer is replayed locally, oldest first. A message is therefore dropped only if a node holds no route newer than the one the message already used, or a forward fails on the network; both are logged and counted.

**I2, no duplication.** The only message that carries actor messages between nodes in a batch is COMMIT, and it can be retried. The destination applies COMMIT only when it holds prepared state for exactly that epoch, and applying it moves the destination's epoch past it. A retried COMMIT for an epoch the destination has already passed is acknowledged but not applied. After IN_DOUBT resolves as committed, the source drops its copy of the batch instead of forwarding it again.

**I3, single live location.** The destination does not create the actor until COMMIT, and the source stops its copy only after COMMITTED. Between those two moments the source copy is frozen (no messages are delivered to it), so at most one copy ever processes messages. If COMMITTED never arrives the source cannot know which side holds the live copy, so it parks its copy as PAUSED instead of resuming it. QUERY settles the question: a REJECT answer also deletes the destination's prepared state, so a COMMIT still in flight can no longer create a second copy. Epochs only grow, and a migration commits only from the epoch it started at, so two migrations of the same actor cannot interleave.

**Forwarding terminates.** A node forwards a message only if it knows an epoch strictly greater than the one the message carries. Epochs are bounded by the number of moves made so far, so a message cannot circulate forever, even when nodes hold stale routes.

### Supervision properties

**No deadlock.** An actor's lock is held only while it runs `receive`, `preStart`, or while its instance is swapped. Supervision decisions run after the failing `receive` has returned, and restarting a subtree locks one actor at a time. `postStop` runs on the stopping actor's own thread. No code path holds two actors' locks at once, so escalations from many children at the same time cannot deadlock (tested with 20 simultaneous escalations).

**Crash loops terminate.** Each RESTART consumes budget, and an exhausted budget turns into ESCALATE. Each escalation moves one level up the finite tree, and the root guardian always stops. So any sequence of repeated failures ends in finitely many steps.

**The tree stays consistent.** A child is attached to its parent while holding the parent's lock and only if the parent has not stopped, so stopping a parent cannot miss a child created at the same moment. Stopping cascades to the whole subtree, and restarting a parent restarts its whole subtree, so no child keeps state tied to a parent instance that no longer exists.

**Lifecycle hooks pair up.** `postStop` runs exactly once for each instance whose `preStart` ran. An actor stopped before its thread ever started runs neither.

## Testing

- `SupervisionTest`: 18 tests covering restart with fresh state and kept mailbox, the sliding restart budget (using a fake clock), failing `preStart`, STOP with dead letters, explicit subtree stop, budget exhaustion escalating to a parent that restarts the subtree, escalation to a stopping parent, multi-level cascades, the root guardian, 20 simultaneous escalations without deadlock, 50 concurrent actors, actors spawning their own children, failing lifecycle hooks, and policy validation.
- Migration: 64 tests. Fault injection through `FaultyTransport` drops chosen protocol messages (PREPARED, COMMIT, COMMITTED, QUERY) to drive every abort and recovery path over real sockets. Every migration test checks the invariants with numbered messages: count equals n (no loss), sum equals n(n+1)/2 (no duplication), zero out of order. One test migrates an actor while 3,000 messages keep arriving.
- Coverage (JaCoCo): supervision classes 95% instructions and 88% branches; `migration` package 98% instructions and 97% branches.
