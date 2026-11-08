# Anusha's Work: Actor Core

## Phase 2 (Current)

### Actor Core Foundation
- Actor base class with receive() lifecycle
- ActorRef: immutable actor reference with UUID
- ActorSystem: manages actor lifecycle and routing
- ActorRunner: polls mailbox and dispatches messages
- Mailbox: BlockingQueue-based async message delivery
- Message: envelope with sender, recipient, payload, timestamp

### Phase 3 (Next)
- Supervision tree implementation
- Restart budget and escalation logic
- Correctness proof documentation
- Failure injection testing
