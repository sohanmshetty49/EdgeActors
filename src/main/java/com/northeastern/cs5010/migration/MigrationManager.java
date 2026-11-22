package com.northeastern.cs5010.migration;

import com.northeastern.cs5010.migration.HandoffMessage.Type;
import com.northeastern.cs5010.transport.AsyncTransport;
import com.northeastern.cs5010.transport.LocationRegistry;
import com.northeastern.cs5010.transport.LocationRegistry.Status;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Live migration of actors between nodes, one instance per node.
 *
 * <p>The handoff is a two-phase protocol with buffering and fencing:
 * <ol>
 *   <li><b>Freeze.</b> The registry moves the actor ACTIVE to MIGRATING. From
 *       then on new messages are buffered, not delivered. The source waits for
 *       the actor to finish what it already has, then snapshots its state.</li>
 *   <li><b>Prepare.</b> The destination checks it can host the actor, holds
 *       the state under a lease, and answers PREPARED or REJECT.</li>
 *   <li><b>Commit.</b> The source sends COMMIT with every message buffered so
 *       far. The destination installs the actor, delivers that batch in order,
 *       and answers COMMITTED. Only then does the source flip its registry to
 *       the new location at epoch + 1, stop its copy, and forward anything that
 *       arrived meanwhile.</li>
 * </ol>
 *
 * <p>The three invariants and what enforces them:
 * <ul>
 *   <li><b>No loss.</b> Every message is either delivered to the old copy,
 *       carried inside COMMIT, or forwarded after COMMITTED. On abort the
 *       buffer is replayed locally in order.</li>
 *   <li><b>No duplication.</b> A COMMIT is applied at most once: a retried
 *       COMMIT for an epoch the destination already passed is acknowledged
 *       without being applied again.</li>
 *   <li><b>Single live location.</b> The destination never runs the actor
 *       before COMMIT, and the source stops its copy only after COMMITTED. If
 *       COMMITTED never arrives the source parks the actor as PAUSED (IN_DOUBT)
 *       instead of guessing, and {@link #resolve(String)} asks the destination
 *       which way it went.</li>
 * </ul>
 *
 * <p>Forwarded messages carry the sender's epoch, and a node only forwards
 * further when it knows a strictly newer epoch. Epochs only grow, so stale
 * routing information can never bounce a message between nodes forever.
 *
 * <p><b>Ordered, retried forwarding.</b> Every message to a peer goes through
 * one ordered outbox per peer. A forwarded message that cannot be sent because
 * the link is down is kept and retried, in order, until it goes through, so a
 * network partition delays forwarded messages instead of losing them. A
 * client's own send still fails with an IOException when the link is down, so
 * the client knows that message was not accepted. Protocol messages never jump
 * ahead of waiting forwards; while the outbox is not empty they are dropped,
 * which the protocol already handles through its timeouts.
 *
 * <p><b>Drain fence.</b> After COMMITTED the old node forwards what it still
 * buffered and then sends DRAINED. Until DRAINED arrives, the new node holds
 * messages from its own clients instead of delivering them, so a message sent
 * before the move, which travels through the old node, is never overtaken by a
 * later one sent directly. If DRAINED never comes, the held messages are
 * released after the drain timeout.
 */
public final class MigrationManager implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(MigrationManager.class.getName());

    public enum Outcome {
        COMMITTED, ABORTED, IN_DOUBT
    }

    /** What happened to one migration, and how long it took end to end. */
    public record Result(Outcome outcome, String reason, long durationMillis) {
        public boolean committed() {
            return outcome == Outcome.COMMITTED;
        }
    }

    /**
     * Tuning knobs. replyTimeout bounds each wait for the destination,
     * commitAttempts is how often COMMIT is retried, idleTimeout bounds the
     * freeze step, prepareLease is how long a destination holds prepared state,
     * drainTimeout is how long a new location waits for DRAINED before it
     * releases the messages it is holding.
     */
    public record Config(Duration replyTimeout, int commitAttempts, Duration idleTimeout, Duration prepareLease,
                         Duration drainTimeout) {

        public static final Duration DEFAULT_DRAIN_TIMEOUT = Duration.ofSeconds(10);

        public Config {
            Objects.requireNonNull(replyTimeout, "replyTimeout");
            Objects.requireNonNull(idleTimeout, "idleTimeout");
            Objects.requireNonNull(prepareLease, "prepareLease");
            Objects.requireNonNull(drainTimeout, "drainTimeout");
            if (commitAttempts < 1) {
                throw new IllegalArgumentException("commitAttempts must be at least 1");
            }
        }

        /** Same, with the default drain timeout. */
        public Config(Duration replyTimeout, int commitAttempts, Duration idleTimeout, Duration prepareLease) {
            this(replyTimeout, commitAttempts, idleTimeout, prepareLease, DEFAULT_DRAIN_TIMEOUT);
        }

        public static Config defaults() {
            return new Config(Duration.ofSeconds(2), 3, Duration.ofSeconds(5), Duration.ofSeconds(15));
        }
    }

    private record Prepared(String actorType, String state, long epoch) {
    }

    private record InDoubt(String destination, long epoch, List<String> batch) {
    }

    private record Route(String node, long epoch) {
    }

    /** A request waiting for its reply, matched by requestId so late replies are ignored. */
    private record Pending(long requestId, CompletableFuture<HandoffMessage> reply) {
    }

    /** One encoded message waiting in a peer's outbox. */
    private record Outgoing(String encoded, boolean forward) {
    }

    /** What to do with a message that cannot be sent right now. */
    private enum OnFailure {
        /** Tell the caller: a client's own send. */
        THROW,
        /** Keep it and retry in order: forwarded messages and DRAINED. */
        QUEUE,
        /** Give up quietly: protocol requests and replies, which have their own timeouts. */
        DROP
    }

    /**
     * Messages held on a new location until the old one says it has forwarded
     * everything it buffered. Only touched under lockFor(actorId).
     */
    private static final class Fence {
        final String from;
        final long epoch;
        final List<String> held = new ArrayList<>();

        Fence(String from, long epoch) {
            this.from = from;
            this.epoch = epoch;
        }
    }

    private static final long RETRY_MILLIS = 100;

    private final String nodeName;
    private final ActorHost host;
    private final LocationRegistry registry;
    private final AsyncTransport transport;
    private final Map<String, String> peers;
    private final Config config;

    private final Map<String, Object> locks = new ConcurrentHashMap<>();
    private final Map<String, List<String>> buffers = new ConcurrentHashMap<>();
    private final Map<String, Pending> awaiting = new ConcurrentHashMap<>();
    private final AtomicLong requestIds = new AtomicLong();
    private final Map<String, Prepared> prepared = new ConcurrentHashMap<>();
    private final Map<String, InDoubt> inDoubt = new ConcurrentHashMap<>();
    private final Map<String, Fence> fences = new ConcurrentHashMap<>();
    private final Map<String, Deque<Outgoing>> outbox = new ConcurrentHashMap<>();
    private final Map<String, Object> outboxLocks = new ConcurrentHashMap<>();
    private final Set<String> retrying = ConcurrentHashMap.newKeySet();
    private final ExecutorService inbound;
    private final ScheduledExecutorService timers;
    private final ScheduledExecutorService retrier;

    private final AtomicLong forwarded = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();
    private final AtomicLong commits = new AtomicLong();
    private final AtomicLong aborts = new AtomicLong();
    private final AtomicLong retried = new AtomicLong();

    /**
     * @param peers maps every other node's name to its "host:port" address
     */
    public MigrationManager(ActorHost host, LocationRegistry registry, AsyncTransport transport,
                            Map<String, String> peers, Config config) {
        this.host = Objects.requireNonNull(host, "host");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.peers = Map.copyOf(peers);
        this.config = Objects.requireNonNull(config, "config");
        this.nodeName = transport.nodeName();
        // One inbound thread: protocol messages from a peer are handled in the
        // order they arrived on its connection, so COMMIT always precedes the
        // FORWARDs that follow it.
        this.inbound = Executors.newSingleThreadExecutor(daemon(nodeName + "-handoff"));
        this.timers = Executors.newSingleThreadScheduledExecutor(daemon(nodeName + "-lease"));
        this.retrier = Executors.newSingleThreadScheduledExecutor(daemon(nodeName + "-retry"));
        transport.setListener((sender, payload) -> inbound.execute(() -> handle(sender, payload)));
    }

    public String nodeName() {
        return nodeName;
    }

    // ------------------------------------------------------------------ API

    /** Creates a new actor on this node and registers it here at epoch 0. */
    public void spawn(String actorId, String actorType) {
        synchronized (lockFor(actorId)) {
            String existing = registry.getLocation(actorId);
            if (existing != null) {
                // Reusing an id that lives elsewhere would create a second actor with the same identity.
                throw new IllegalStateException(actorId + " already exists on " + existing);
            }
            host.install(actorId, actorType, null);
            registry.register(actorId, nodeName);
        }
    }

    /** Records that an actor lives on another node, so this node can route to it. */
    public void learn(String actorId, String node, long epoch) {
        if (!peers.containsKey(node) && !nodeName.equals(node)) {
            throw new IllegalArgumentException("unknown node " + node);
        }
        synchronized (lockFor(actorId)) {
            registry.register(actorId, node, epoch);
        }
    }

    /**
     * Sends a message to an actor wherever it lives: delivered here if it runs
     * here, buffered if it is migrating, forwarded if it lives elsewhere.
     */
    public void send(String actorId, String payload) throws IOException {
        Route route;
        synchronized (lockFor(actorId)) {
            if (registry.getLocation(actorId) == null) {
                throw new IllegalArgumentException("unknown actor " + actorId);
            }
            route = routeLocked(actorId, payload, null);
        }
        if (route != null) {
            forward(route.node(), actorId, route.epoch(), payload);
        }
    }

    /** Moves an actor that runs on this node to the destination node. Blocks until decided. */
    public Result migrate(String actorId, String destination) {
        long start = System.nanoTime();
        if (nodeName.equals(destination)) {
            return result(Outcome.ABORTED, "destination is this node", start);
        }
        if (!peers.containsKey(destination)) {
            return result(Outcome.ABORTED, "unknown destination " + destination, start);
        }
        // An actor that just arrived may still be receiving forwards from its old
        // node. Moving it on before that finishes would separate the held messages
        // from the ones still coming, so wait for the drain, bounded by idleTimeout.
        long drainDeadline = System.nanoTime() + config.idleTimeout().toNanos();
        while (fences.containsKey(actorId) && System.nanoTime() < drainDeadline) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return result(Outcome.ABORTED, "interrupted while waiting for the previous handoff to drain", start);
            }
        }
        long epoch;
        String type;
        synchronized (lockFor(actorId)) {
            Fence fence = fences.get(actorId);
            if (fence != null) {
                return result(Outcome.ABORTED, actorId + " is still receiving messages forwarded from "
                    + fence.from + "; try again shortly", start);
            }
            if (!host.hosts(actorId) || !registry.beginMigration(actorId, nodeName)) {
                return result(Outcome.ABORTED, actorId + " is not active on " + nodeName, start);
            }
            epoch = registry.getEpoch(actorId);
            type = host.typeOf(actorId);
        }

        // Freeze: new messages are buffered from here on.
        String state;
        try {
            if (!host.awaitIdle(actorId, config.idleTimeout())) {
                return abort(actorId, null, epoch, List.of(), "actor did not become idle in time", start);
            }
            state = host.snapshot(actorId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return abort(actorId, null, epoch, List.of(), "interrupted while freezing", start);
        } catch (RuntimeException e) {
            return abort(actorId, null, epoch, List.of(), "snapshot failed: " + e.getMessage(), start);
        }

        // Prepare.
        HandoffMessage reply = request(actorId, destination, HandoffMessage.prepare(actorId, type, epoch, state));
        if (!isReply(reply, Type.PREPARED, epoch)) {
            return abort(actorId, destination, epoch, List.of(), explain("PREPARE", destination, reply), start);
        }

        // Commit, carrying everything buffered so far. Later arrivals keep buffering.
        List<String> batch;
        synchronized (lockFor(actorId)) {
            batch = drainBuffer(actorId);
        }
        HandoffMessage commit = HandoffMessage.commit(actorId, epoch, batch);
        for (int attempt = 0; attempt < config.commitAttempts(); attempt++) {
            reply = request(actorId, destination, commit);
            if (reply == null) {
                continue; // lost or slow; COMMIT is safe to repeat
            }
            if (isReply(reply, Type.COMMITTED, epoch)) {
                return finishCommit(actorId, destination, epoch, start);
            }
            return abort(actorId, destination, epoch, batch, explain("COMMIT", destination, reply), start);
        }

        // No answer at all: the destination may or may not have installed the
        // actor. Running it here could create a second live copy, so park it.
        synchronized (lockFor(actorId)) {
            registry.setStatus(actorId, Status.PAUSED);
            inDoubt.put(actorId, new InDoubt(destination, epoch, batch));
        }
        return result(Outcome.IN_DOUBT, "no reply to COMMIT after " + config.commitAttempts()
            + " attempts; actor paused so it can never run in two places", start);
    }

    /**
     * Settles an IN_DOUBT migration by asking the destination whether it
     * committed. Finishes the move if it did, restores the actor here if not.
     */
    public Result resolve(String actorId) {
        long start = System.nanoTime();
        InDoubt doubt = inDoubt.get(actorId);
        if (doubt == null) {
            return result(Outcome.ABORTED, "no in-doubt handoff for " + actorId, start);
        }
        HandoffMessage reply = request(actorId, doubt.destination(), HandoffMessage.query(actorId, doubt.epoch()));
        if (reply == null) {
            return result(Outcome.IN_DOUBT, "destination " + doubt.destination() + " still not answering", start);
        }
        inDoubt.remove(actorId);
        if (isReply(reply, Type.COMMITTED, doubt.epoch())) {
            // The batch already reached the destination inside COMMIT; do not resend it.
            return finishCommit(actorId, doubt.destination(), doubt.epoch(), start);
        }
        return abort(actorId, null, doubt.epoch(), doubt.batch(),
            "destination never committed; restored on " + nodeName, start);
    }

    public boolean isInDoubt(String actorId) {
        return inDoubt.containsKey(actorId);
    }

    public long forwardedCount() {
        return forwarded.get();
    }

    public long droppedCount() {
        return dropped.get();
    }

    public long commitCount() {
        return commits.get();
    }

    public long abortCount() {
        return aborts.get();
    }

    /** Messages sent later than first tried, because the link was down at the time. */
    public long retriedCount() {
        return retried.get();
    }

    /** Messages waiting in outboxes for a link to come back. */
    public int queuedCount() {
        int n = 0;
        for (Map.Entry<String, Deque<Outgoing>> e : outbox.entrySet()) {
            synchronized (outboxLock(e.getKey())) {
                n += e.getValue().size();
            }
        }
        return n;
    }

    /** True while this node holds an arriving actor's local messages until the old node has drained. */
    public boolean isDraining(String actorId) {
        return fences.containsKey(actorId);
    }

    @Override
    public void close() {
        transport.setListener(null);
        inbound.shutdownNow();
        timers.shutdownNow();
        retrier.shutdownNow();
        int waiting = queuedCount();
        if (waiting > 0) {
            LOG.log(System.Logger.Level.WARNING, nodeName + " closed with " + waiting
                + " messages still waiting for an unreachable node");
        }
    }

    // -------------------------------------------------------- source side

    private HandoffMessage request(String actorId, String destination, HandoffMessage message) {
        Pending pending = new Pending(requestIds.incrementAndGet(), new CompletableFuture<>());
        awaiting.put(actorId, pending);
        try {
            if (!sendOrdered(destination, message.withRequestId(pending.requestId()).encode(), false, OnFailure.DROP)) {
                return null; // link down or earlier messages still waiting; the caller treats it as no reply
            }
            return pending.reply().get(config.replyTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (IOException | TimeoutException | ExecutionException e) {
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } finally {
            awaiting.remove(actorId, pending);
        }
    }

    private Result finishCommit(String actorId, String destination, long epoch, long start) {
        synchronized (lockFor(actorId)) {
            if (!registry.commitMigration(actorId, epoch, destination)) {
                // Unreachable while this node holds the actor MIGRATING or PAUSED.
                LOG.log(System.Logger.Level.ERROR, "epoch of " + actorId + " changed during its handoff");
            }
            host.stop(actorId);
            // Forwarding under the lock keeps these ahead of any message sent after the switch.
            for (String payload : drainBuffer(actorId)) {
                forwardQuietly(destination, actorId, epoch + 1, payload);
            }
            // Tells the new location it can deliver its own clients' messages now.
            sendReliably(destination, HandoffMessage.drained(actorId, epoch).encode());
        }
        commits.incrementAndGet();
        return result(Outcome.COMMITTED, "", start);
    }

    private Result abort(String actorId, String destination, long epoch, List<String> batch,
                         String reason, long start) {
        if (destination != null) {
            sendQuietly(destination, HandoffMessage.abort(actorId, epoch));
        }
        synchronized (lockFor(actorId)) {
            registry.setStatus(actorId, Status.ACTIVE);
            // Older messages first, then whatever arrived while we were deciding.
            for (String payload : batch) {
                host.deliver(actorId, payload);
            }
            for (String payload : drainBuffer(actorId)) {
                host.deliver(actorId, payload);
            }
        }
        aborts.incrementAndGet();
        return result(Outcome.ABORTED, reason, start);
    }

    // --------------------------------------------------- destination side

    private void handle(String sender, String raw) {
        HandoffMessage msg;
        try {
            msg = HandoffMessage.decode(raw);
        } catch (IllegalArgumentException e) {
            dropped.incrementAndGet();
            LOG.log(System.Logger.Level.WARNING, "ignoring malformed handoff message from " + sender);
            return;
        }
        switch (msg.type()) {
            case PREPARE -> onPrepare(sender, msg);
            case COMMIT -> onCommit(sender, msg);
            case ABORT -> {
                synchronized (lockFor(msg.actorId())) {
                    discardPrepared(msg.actorId(), msg.epoch());
                }
            }
            case QUERY -> onQuery(sender, msg);
            case FORWARD -> onForward(sender, msg);
            case DRAINED -> onDrained(sender, msg);
            case PREPARED, REJECT, COMMITTED -> {
                Pending pending = awaiting.get(msg.actorId());
                // Only the answer to the request in flight counts; a late reply
                // to an earlier attempt for the same actor is ignored.
                if (pending != null && pending.requestId() == msg.requestId()) {
                    pending.reply().complete(msg);
                }
            }
        }
    }

    private void onPrepare(String sender, HandoffMessage msg) {
        String id = msg.actorId();
        String problem;
        synchronized (lockFor(id)) {
            problem = admissionProblem(msg);
            if (problem == null) {
                Prepared held = new Prepared(msg.actorType(), msg.state(), msg.epoch());
                prepared.put(id, held);
                timers.schedule(() -> expireLease(id, held), config.prepareLease().toMillis(), TimeUnit.MILLISECONDS);
            }
        }
        sendQuietly(sender, msg.answer(problem == null
            ? HandoffMessage.prepared(id, msg.epoch())
            : HandoffMessage.reject(id, msg.epoch(), problem)));
    }

    private String admissionProblem(HandoffMessage msg) {
        if (host.hosts(msg.actorId())) {
            return "actor already hosted on " + nodeName;
        }
        if (prepared.containsKey(msg.actorId())) {
            return "another handoff of this actor is in progress";
        }
        if (!host.canInstall(msg.actorType())) {
            return "no factory for actor type " + msg.actorType() + " on " + nodeName;
        }
        return null;
    }

    private void expireLease(String id, Prepared held) {
        synchronized (lockFor(id)) {
            if (prepared.get(id) == held) {
                prepared.remove(id);
            }
        }
    }

    private void onCommit(String sender, HandoffMessage msg) {
        String id = msg.actorId();
        HandoffMessage reply;
        synchronized (lockFor(id)) {
            if (alreadyCommitted(id, msg.epoch())) {
                // A retried COMMIT: acknowledge again, never apply twice.
                reply = HandoffMessage.committed(id, msg.epoch());
            } else {
                Prepared held = prepared.get(id);
                if (held == null || held.epoch() != msg.epoch()) {
                    reply = HandoffMessage.reject(id, msg.epoch(), "no prepared state for epoch " + msg.epoch());
                } else {
                    prepared.remove(id);
                    reply = install(id, held, msg, sender);
                }
            }
        }
        sendQuietly(sender, msg.answer(reply));
    }

    /** Caller holds lockFor(id). */
    private HandoffMessage install(String id, Prepared held, HandoffMessage commit, String from) {
        try {
            host.install(id, held.actorType(), held.state());
        } catch (RuntimeException e) {
            return HandoffMessage.reject(id, commit.epoch(), "install failed: " + e.getMessage());
        }
        registry.register(id, nodeName, commit.epoch() + 1);
        for (String payload : commit.messages()) {
            host.deliver(id, payload);
        }
        // Hold this node's own messages until the old node has forwarded everything it still has.
        Fence fence = new Fence(from, commit.epoch());
        fences.put(id, fence);
        try {
            timers.schedule(() -> openFence(id, fence, true), config.drainTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException closed) {
            fences.remove(id, fence);
        }
        return HandoffMessage.committed(id, commit.epoch());
    }

    private void onDrained(String sender, HandoffMessage msg) {
        Fence fence = fences.get(msg.actorId());
        if (fence != null && fence.from.equals(sender) && fence.epoch == msg.epoch()) {
            openFence(msg.actorId(), fence, false);
        }
    }

    /** Delivers what the fence held, in order, and removes it. */
    private void openFence(String id, Fence fence, boolean timedOut) {
        synchronized (lockFor(id)) {
            if (!fences.remove(id, fence)) {
                return; // already opened
            }
            if (timedOut) {
                LOG.log(System.Logger.Level.WARNING, "no DRAINED from " + fence.from + " for " + id + " within "
                    + config.drainTimeout() + "; releasing " + fence.held.size() + " held messages");
            }
            for (String payload : fence.held) {
                try {
                    host.deliver(id, payload);
                } catch (IllegalArgumentException gone) {
                    dropped.incrementAndGet();
                    LOG.log(System.Logger.Level.ERROR, id + " is no longer hosted; dropped a held message");
                }
            }
        }
    }

    private void onQuery(String sender, HandoffMessage msg) {
        String id = msg.actorId();
        HandoffMessage reply;
        synchronized (lockFor(id)) {
            if (alreadyCommitted(id, msg.epoch())) {
                reply = HandoffMessage.committed(id, msg.epoch());
            } else {
                // Discarding now means a late COMMIT for this epoch can never apply.
                discardPrepared(id, msg.epoch());
                reply = HandoffMessage.reject(id, msg.epoch(), "handoff was never committed on " + nodeName);
            }
        }
        sendQuietly(sender, msg.answer(reply));
    }

    private void onForward(String sender, HandoffMessage msg) {
        String id = msg.actorId();
        String payload = msg.messages().isEmpty() ? "" : msg.messages().get(0);
        Route route;
        synchronized (lockFor(id)) {
            String location = registry.getLocation(id);
            boolean knowsNewerRoute = location != null
                && (location.equals(nodeName) || registry.getEpoch(id) > msg.epoch());
            if (!knowsNewerRoute) {
                dropped.incrementAndGet();
                LOG.log(System.Logger.Level.WARNING,
                    "dropping message for " + id + ": no route newer than epoch " + msg.epoch());
                return;
            }
            route = routeLocked(id, payload, sender);
        }
        if (route != null) {
            forwardQuietly(route.node(), id, route.epoch(), payload);
        }
    }

    /** The destination has moved past this epoch, so the COMMIT for it was applied. */
    private boolean alreadyCommitted(String id, long epoch) {
        return registry.getEpoch(id) > epoch;
    }

    private void discardPrepared(String id, long epoch) {
        Prepared held = prepared.get(id);
        if (held != null && held.epoch() == epoch) {
            prepared.remove(id);
        }
    }

    // ------------------------------------------------------------ helpers

    /**
     * Caller holds lockFor(actorId) and has checked the actor is known.
     *
     * @param arrivedFrom the node that forwarded this message, or null for a local client
     */
    private Route routeLocked(String actorId, String payload, String arrivedFrom) {
        String location = registry.getLocation(actorId);
        if (location.equals(nodeName)) {
            if (registry.getStatus(actorId) == Status.ACTIVE) {
                Fence fence = fences.get(actorId);
                if (fence != null && !fence.from.equals(arrivedFrom)) {
                    fence.held.add(payload); // the old node is still forwarding older messages
                } else {
                    host.deliver(actorId, payload);
                }
            } else {
                buffers.computeIfAbsent(actorId, k -> new ArrayList<>()).add(payload);
            }
            return null;
        }
        return new Route(location, registry.getEpoch(actorId));
    }

    private List<String> drainBuffer(String actorId) {
        List<String> buffered = buffers.remove(actorId);
        return buffered == null ? new ArrayList<>() : buffered;
    }

    /** A client's message: sent in order, or an IOException so the client knows it was not accepted. */
    private void forward(String node, String actorId, long epoch, String payload) throws IOException {
        sendOrdered(node, HandoffMessage.forward(actorId, epoch, payload).encode(), true, OnFailure.THROW);
    }

    /** A message this node already accepted: sent in order, and retried while the link is down. */
    private void forwardQuietly(String node, String actorId, long epoch, String payload) {
        try {
            sendOrdered(node, HandoffMessage.forward(actorId, epoch, payload).encode(), true, OnFailure.QUEUE);
        } catch (IOException e) {
            dropped.incrementAndGet(); // only for a node with no known address
            LOG.log(System.Logger.Level.ERROR, "could not forward a message for " + actorId + " to " + node, e);
        }
    }

    private void sendReliably(String node, String encoded) {
        try {
            sendOrdered(node, encoded, false, OnFailure.QUEUE);
        } catch (IOException e) {
            LOG.log(System.Logger.Level.ERROR, "no address for " + node, e);
        }
    }

    private void sendQuietly(String node, HandoffMessage msg) {
        boolean sent;
        try {
            sent = sendOrdered(node, msg.encode(), false, OnFailure.DROP);
        } catch (IOException e) {
            sent = false;
        }
        if (!sent) {
            LOG.log(System.Logger.Level.WARNING, "could not send " + msg.type() + " to " + node);
        }
    }

    /**
     * Sends to a peer through its ordered outbox. Anything already waiting is
     * retried first, and a new message never overtakes a waiting one. Returns
     * true if the message was sent, or kept for retry in QUEUE mode.
     */
    private boolean sendOrdered(String node, String encoded, boolean forward, OnFailure onFailure)
            throws IOException {
        String address = peers.get(node);
        if (address == null) {
            if (onFailure == OnFailure.DROP) {
                return false;
            }
            throw new IOException("no address known for node " + node);
        }
        synchronized (outboxLock(node)) {
            Deque<Outgoing> waiting = outbox.computeIfAbsent(node, k -> new ArrayDeque<>());
            flushLocked(address, waiting);
            if (waiting.isEmpty()) {
                try {
                    transport.send(address, encoded);
                    if (forward) {
                        forwarded.incrementAndGet();
                    }
                    return true;
                } catch (IOException e) {
                    if (onFailure == OnFailure.THROW) {
                        throw e;
                    }
                    if (onFailure == OnFailure.DROP) {
                        return false;
                    }
                }
            } else if (onFailure == OnFailure.THROW) {
                throw new IOException("link to " + node + " is down: " + waiting.size()
                    + " earlier messages are still waiting");
            } else if (onFailure == OnFailure.DROP) {
                return false;
            }
            waiting.addLast(new Outgoing(encoded, forward));
            scheduleRetry(node);
            return true;
        }
    }

    /** Sends waiting messages in order until one fails. Caller holds outboxLock(node). */
    private void flushLocked(String address, Deque<Outgoing> waiting) {
        while (!waiting.isEmpty()) {
            Outgoing next = waiting.peekFirst();
            try {
                transport.send(address, next.encoded());
            } catch (IOException e) {
                return; // still down; try again later
            }
            waiting.pollFirst();
            retried.incrementAndGet();
            if (next.forward()) {
                forwarded.incrementAndGet();
            }
        }
    }

    private void scheduleRetry(String node) {
        if (retrying.add(node)) {
            try {
                retrier.schedule(() -> retry(node), RETRY_MILLIS, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException closed) {
                retrying.remove(node);
            }
        }
    }

    private void retry(String node) {
        retrying.remove(node);
        synchronized (outboxLock(node)) {
            Deque<Outgoing> waiting = outbox.get(node);
            if (waiting == null || waiting.isEmpty()) {
                return;
            }
            flushLocked(peers.get(node), waiting);
            if (!waiting.isEmpty()) {
                scheduleRetry(node);
            }
        }
    }

    private Object outboxLock(String node) {
        return outboxLocks.computeIfAbsent(node, k -> new Object());
    }

    private Object lockFor(String actorId) {
        return locks.computeIfAbsent(actorId, k -> new Object());
    }

    private static boolean isReply(HandoffMessage reply, Type expected, long epoch) {
        return reply != null && reply.type() == expected && reply.epoch() == epoch;
    }

    private static String explain(String phase, String destination, HandoffMessage reply) {
        if (reply == null) {
            return "no reply to " + phase + " from " + destination;
        }
        return destination + " answered " + reply.type()
            + (reply.reason().isEmpty() ? "" : ": " + reply.reason());
    }

    private static Result result(Outcome outcome, String reason, long startNanos) {
        return new Result(outcome, reason, (System.nanoTime() - startNanos) / 1_000_000);
    }

    private static ThreadFactory daemon(String name) {
        return r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        };
    }
}
