package com.northeastern.cs5010.migration;

import com.northeastern.cs5010.transport.FramedTransport;
import com.northeastern.cs5010.transport.LocationRegistry;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Text commands for driving a node by hand, used for the Pi demo. Every
 * command returns its output as a string, so the shell is testable without a
 * terminal.
 */
public final class NodeShell {

    static final String HELP = String.join("\n",
        "spawn <actor> [type]          start an actor here (type defaults to counter)",
        "send <actor> <count>          send numbered messages 1, 2, 3, ... (continues across calls)",
        "tell <actor> <text>           send one free-text message",
        "migrate <actor> <node>        move an actor that runs here to another node",
        "resolve <actor>               settle an IN_DOUBT migration",
        "learn <actor> <node> [epoch]  record that an actor lives on another node",
        "status <actor>                location, epoch, state and, if hosted here, the counter",
        "metrics <actor>               current node, mailbox depth, processed, mean processing time",
        "stats                         transport and migration counters, memory, cores",
        "quit                          stop this node");

    private final MigrationManager manager;
    private final LocalActorHost host;
    private final LocationRegistry registry;
    private final FramedTransport transport;
    private final Map<String, Long> nextSeq = new HashMap<>();

    public NodeShell(MigrationManager manager, LocalActorHost host, LocationRegistry registry,
                     FramedTransport transport) {
        this.manager = manager;
        this.host = host;
        this.registry = registry;
        this.transport = transport;
    }

    public String execute(String line) {
        String[] t = line.trim().split("\\s+");
        if (t[0].isEmpty()) {
            return "";
        }
        try {
            return switch (t[0]) {
                case "help" -> HELP;
                case "spawn" -> spawn(t);
                case "send" -> send(t);
                case "tell" -> tell(line, t);
                case "migrate" -> describe(manager.migrate(arg(t, 1, "migrate <actor> <node>"),
                    arg(t, 2, "migrate <actor> <node>")));
                case "resolve" -> describe(manager.resolve(arg(t, 1, "resolve <actor>")));
                case "learn" -> learn(t);
                case "status" -> status(arg(t, 1, "status <actor>"));
                case "metrics" -> metrics(arg(t, 1, "metrics <actor>"));
                case "stats" -> stats();
                default -> "unknown command '" + t[0] + "', type help";
            };
        } catch (IllegalArgumentException | IllegalStateException e) {
            return "error: " + e.getMessage();
        } catch (IOException e) {
            return "network error: " + e.getMessage();
        }
    }

    private String spawn(String[] t) {
        String id = arg(t, 1, "spawn <actor> [type]");
        String type = t.length > 2 ? t[2] : CounterActor.TYPE;
        manager.spawn(id, type);
        return "spawned " + id + " (" + type + ") on " + manager.nodeName();
    }

    private String send(String[] t) throws IOException {
        String usage = "send <actor> <count>";
        String id = arg(t, 1, usage);
        long count = Long.parseLong(arg(t, 2, usage));
        if (count < 1) {
            throw new IllegalArgumentException("count must be at least 1");
        }
        long first = nextSeq.getOrDefault(id, 1L);
        for (long seq = first; seq < first + count; seq++) {
            manager.send(id, Long.toString(seq));
        }
        nextSeq.put(id, first + count);
        return "sent " + count + " messages to " + id + " (" + first + ".." + (first + count - 1) + ")";
    }

    private String tell(String line, String[] t) throws IOException {
        String id = arg(t, 1, "tell <actor> <text>");
        arg(t, 2, "tell <actor> <text>");
        String text = line.trim().split("\\s+", 3)[2];
        manager.send(id, text);
        return "sent to " + id;
    }

    private String learn(String[] t) {
        String usage = "learn <actor> <node> [epoch]";
        String id = arg(t, 1, usage);
        String node = arg(t, 2, usage);
        long epoch = t.length > 3 ? Long.parseLong(t[3]) : 0;
        manager.learn(id, node, epoch);
        return id + " is on " + node + " at epoch " + epoch;
    }

    private String status(String id) {
        String location = registry.getLocation(id);
        if (location == null) {
            return id + " is unknown on " + manager.nodeName();
        }
        StringBuilder out = new StringBuilder()
            .append(id).append(": location=").append(location)
            .append(" epoch=").append(registry.getEpoch(id))
            .append(" status=").append(registry.getStatus(id));
        if (manager.isInDoubt(id)) {
            out.append(" in-doubt (run resolve ").append(id).append(')');
        }
        if (host.hosts(id)) {
            String state = host.inspect(id, actor -> actor instanceof CounterActor c
                ? " count=" + c.count() + " sum=" + c.sum() + " outOfOrder=" + c.outOfOrder()
                : " state=" + actor.snapshot());
            out.append(state);
        }
        return out.toString();
    }

    private String metrics(String id) {
        String location = registry.getLocation(id);
        if (location == null) {
            return id + " is unknown on " + manager.nodeName();
        }
        if (!host.hosts(id)) {
            String reason = host.stoppedReason(id);
            if (reason != null) {
                return id + ": node=" + location + " stopped by its supervisor (" + reason + ")";
            }
            return id + ": node=" + location + " (metrics are kept on that node)";
        }
        LocalActorHost.ActorMetrics m = host.metrics(id);
        return String.format(java.util.Locale.ROOT,
            "%s: node=%s mailbox=%d processed=%d avgProcessingMicros=%.1f failures=%d restarts=%d",
            id, location, m.mailboxDepth(), m.processed(), m.averageProcessingMicros(), m.failures(), m.restarts());
    }

    private String stats() {
        Runtime rt = Runtime.getRuntime();
        long usedMb = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
        return "node=" + manager.nodeName()
            + " framesSent=" + transport.framesSent()
            + " framesReceived=" + transport.framesReceived()
            + " forwarded=" + manager.forwardedCount()
            + " dropped=" + manager.droppedCount()
            + " commits=" + manager.commitCount()
            + " aborts=" + manager.abortCount()
            + " heapUsedMb=" + usedMb
            + " heapMaxMb=" + rt.maxMemory() / (1024 * 1024)
            + " cores=" + rt.availableProcessors()
            + " deadLetters=" + host.deadLetterCount();
    }

    static String describe(MigrationManager.Result r) {
        return r.outcome() + " in " + r.durationMillis() + " ms"
            + (r.reason().isEmpty() ? "" : " (" + r.reason() + ")");
    }

    private static String arg(String[] t, int index, String usage) {
        if (index >= t.length) {
            throw new IllegalArgumentException("usage: " + usage);
        }
        return t[index];
    }
}
