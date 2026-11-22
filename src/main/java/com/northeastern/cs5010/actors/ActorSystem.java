package com.northeastern.cs5010.actors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * ActorSystem manages actor lifecycle, message routing, and supervision.
 *
 * <p>Actors form a tree. Top-level actors are supervised by the system's root
 * guardian; children are supervised by their parent actor. When an actor's
 * receive throws, its supervision policy decides:
 * <ul>
 *   <li><b>RESTART</b>: discard the instance and create a fresh one. The
 *       mailbox is kept, so only the failing message is lost. Restarting an
 *       actor restarts its whole subtree. Each restart uses the restart
 *       budget; when the budget is exhausted the failure escalates.</li>
 *   <li><b>STOP</b>: stop the actor and its whole subtree.</li>
 *   <li><b>ESCALATE</b>: the parent fails with this failure, and the parent's
 *       own policy decides, cascading upward. A failure that reaches the root
 *       guardian stops the top-level actor.</li>
 * </ul>
 *
 * <p>Every actor runs on its own virtual thread, so the system scales to
 * thousands of actors on a Raspberry Pi without one OS thread per actor.
 */
public class ActorSystem {
    private static final System.Logger LOG = System.getLogger(ActorSystem.class.getName());

    private final Map<String, ActorCell> cells = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final String nodeName;
    private final LongSupplier clock;
    private final AtomicLong deadLetters = new AtomicLong();
    private final AtomicLong rootStops = new AtomicLong();

    public ActorSystem(String nodeName) {
        this(nodeName, System::nanoTime);
    }

    /** clock returns nanoseconds and drives restart budgets; tests pass a fake clock. */
    ActorSystem(String nodeName, LongSupplier clock) {
        this.nodeName = nodeName;
        this.clock = clock;
    }

    // ------------------------------------------------------------ creation

    /** Create and start a new top-level actor with the default supervision policy. */
    public ActorRef actorOf(Class<? extends Actor> actorClass, String name) throws Exception {
        return actorOf(actorClass, name, Supervision.DEFAULT);
    }

    /** Create and start a new top-level actor, supervised by the root guardian with this policy. */
    public ActorRef actorOf(Class<? extends Actor> actorClass, String name, Supervision supervision)
            throws Exception {
        return spawn(actorClass, name, null, supervision);
    }

    /** Create and start a child of a running actor; the parent supervises it with this policy. */
    public ActorRef spawnChild(ActorRef parent, Class<? extends Actor> actorClass, String name,
                               Supervision supervision) throws Exception {
        ActorCell parentCell = cells.get(parent.getId());
        if (parentCell == null) {
            throw new IllegalStateException("parent is not running: " + parent);
        }
        return spawn(actorClass, name, parentCell, supervision);
    }

    private ActorRef spawn(Class<? extends Actor> actorClass, String name, ActorCell parent,
                           Supervision supervision) throws Exception {
        Actor instance = actorClass.getDeclaredConstructor().newInstance();
        ActorRef ref = new ActorRef(name, nodeName);
        RestartBudget budget = new RestartBudget(supervision.maxRestarts(), supervision.window(), clock);
        ActorCell cell = new ActorCell(ref, actorClass, parent, supervision, budget);
        cell.install(instance, this);
        if (parent != null) {
            synchronized (parent) {
                // Checked under the parent's lock so a stopping parent cannot miss this child.
                if (parent.stopped) {
                    throw new IllegalStateException("parent is not running: " + parent.ref);
                }
                parent.children.add(cell);
            }
        }
        cells.put(ref.getId(), cell);
        executor.execute(new ActorRunner(cell, this));
        return ref;
    }

    // ------------------------------------------------------------ messaging

    /**
     * Send a message to an actor. Messages to a stopped or unknown actor are
     * counted as dead letters instead of being delivered.
     */
    public void send(ActorRef recipient, Message message) throws InterruptedException {
        ActorCell cell = cells.get(recipient.getId());
        if (cell == null || cell.stopped) {
            deadLetters.incrementAndGet();
            return;
        }
        cell.mailbox.enqueue(message);
    }

    /**
     * Get mailbox depth for monitoring.
     */
    public int getMailboxDepth(ActorRef ref) {
        ActorCell cell = cells.get(ref.getId());
        return cell != null ? cell.mailbox.depth() : 0;
    }

    // ---------------------------------------------------------- supervision

    /**
     * Applies the failing actor's supervision policy. Runs on the failing
     * actor's thread after its receive returned, holding no cell lock.
     */
    void handleFailure(ActorCell cell, Throwable cause) {
        cell.failures.incrementAndGet();
        cell.lastFailure = cause;
        Supervision.Directive directive = cell.supervision.directive();
        if (directive == Supervision.Directive.RESTART && !cell.budget.tryAcquire()) {
            directive = Supervision.Directive.ESCALATE; // restart budget used up
        }
        switch (directive) {
            case RESTART -> restartTree(cell);
            case STOP -> stop(cell);
            case ESCALATE -> escalate(cell, cause);
        }
    }

    private void escalate(ActorCell cell, Throwable cause) {
        if (cell.parent == null) {
            // Root guardian: nobody above can handle it, so the top-level actor stops.
            rootStops.incrementAndGet();
            LOG.log(System.Logger.Level.WARNING, "stopping " + cell.ref.getPath() + " after unhandled failure", cause);
            stop(cell);
            return;
        }
        // The parent fails with the child's error; the child is restarted or
        // stopped as part of whatever happens to the parent.
        handleFailure(cell.parent, new EscalatedFailure(cell.ref, cause));
    }

    private void restartTree(ActorCell cell) {
        RuntimeException startFailure = replaceInstance(cell);
        if (startFailure != null) {
            // The fresh instance failed in preStart: another failure, which uses budget too.
            handleFailure(cell, startFailure);
            return;
        }
        for (ActorCell child : List.copyOf(cell.children)) {
            restartTree(child);
        }
    }

    /** Swaps in a fresh instance under the cell's lock. Returns a preStart failure, if any. */
    private RuntimeException replaceInstance(ActorCell cell) {
        synchronized (cell) {
            if (cell.stopped) {
                return null;
            }
            runHook(cell, cell.instance()::postStop);
            Actor fresh;
            try {
                fresh = cell.type.getDeclaredConstructor().newInstance();
            } catch (ReflectiveOperationException e) {
                return new IllegalStateException("cannot recreate " + cell.ref.getPath(), e);
            }
            cell.install(fresh, this);
            cell.restarts.incrementAndGet();
            try {
                fresh.preStart();
                return null;
            } catch (RuntimeException e) {
                return e;
            }
        }
    }

    // ------------------------------------------------------------ stopping

    /** Stops an actor and its whole subtree. No effect if it is already stopped. */
    public void stop(ActorRef ref) {
        ActorCell cell = cells.get(ref.getId());
        if (cell != null) {
            stop(cell);
        }
    }

    private void stop(ActorCell cell) {
        List<ActorCell> children;
        synchronized (cell) {
            if (cell.stopped) {
                return;
            }
            cell.stopped = true;
            children = List.copyOf(cell.children);
        }
        for (ActorCell child : children) {
            stop(child);
        }
        // The runner thread wakes up, sees stopped, and runs postStop itself.
        cell.wakeRunner();
        cells.remove(cell.ref.getId());
        if (cell.parent != null) {
            cell.parent.children.remove(cell);
        }
        while (cell.mailbox.poll() != null) {
            deadLetters.incrementAndGet(); // queued but never processed
        }
    }

    /**
     * Shutdown the actor system: stops every actor, then waits briefly for
     * their threads to finish.
     */
    public void shutdown() {
        for (ActorCell cell : new ArrayList<>(cells.values())) {
            if (cell.parent == null) {
                stop(cell);
            }
        }
        executor.shutdown();
        try {
            executor.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void runHook(ActorCell cell, Runnable hook) {
        try {
            hook.run();
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "lifecycle hook failed in " + cell.ref.getPath(), e);
        }
    }

    void recordDeadLetter() {
        deadLetters.incrementAndGet();
    }

    // ----------------------------------------------------------- monitoring

    public boolean isAlive(ActorRef ref) {
        ActorCell cell = cells.get(ref.getId());
        return cell != null && !cell.stopped;
    }

    /** The running children of an actor, empty if it has none or is not running. */
    public List<ActorRef> children(ActorRef ref) {
        ActorCell cell = cells.get(ref.getId());
        if (cell == null) {
            return List.of();
        }
        return cell.children.stream().map(c -> c.ref).toList();
    }

    public long failureCount(ActorRef ref) {
        ActorCell cell = cells.get(ref.getId());
        return cell == null ? 0 : cell.failures.get();
    }

    public long restartCount(ActorRef ref) {
        ActorCell cell = cells.get(ref.getId());
        return cell == null ? 0 : cell.restarts.get();
    }

    /** The most recent failure of a running actor, or null. */
    public Throwable lastFailure(ActorRef ref) {
        ActorCell cell = cells.get(ref.getId());
        return cell == null ? null : cell.lastFailure;
    }

    /** Messages that could not be delivered because their actor was stopped or unknown. */
    public long deadLetterCount() {
        return deadLetters.get();
    }

    /** Top-level actors the root guardian stopped after an unhandled failure. */
    public long rootStopCount() {
        return rootStops.get();
    }
}
