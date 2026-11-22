package com.northeastern.cs5010.migration;

import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Command-line settings for one node.
 *
 * <pre>
 * --name pi                         this node's name (required)
 * --port 9100                       port to listen on (default 9100)
 * --peer mac=192.168.1.20:9100      another node, repeat for each peer
 * --tls certs/pi.p12                enable mutual TLS with this keystore
 * --password secret                 keystore password (or set EDGE_TLS_PASSWORD)
 * --auto-migrate cloud              migrate actors to this peer automatically when overloaded
 * --auto-depth 100                  mailbox depth that counts as overloaded (default 100)
 * --auto-cpu 0.8                    also migrate when CPU load reaches this fraction (default off)
 * </pre>
 *
 * @param autoMigrate the automatic migration policy, or null when it is off
 */
public record NodeConfig(String name, int port, Map<String, String> peers, Path keystore, String keystorePassword,
                         AutoMigrator.Policy autoMigrate) {

    public static final int DEFAULT_AUTO_DEPTH = 100;

    public static final int DEFAULT_PORT = 9100;

    public boolean autoMigration() {
        return autoMigrate != null;
    }

    public boolean tls() {
        return keystore != null;
    }

    public static NodeConfig parse(String[] args, Map<String, String> env) {
        String name = null;
        int port = DEFAULT_PORT;
        Map<String, String> peers = new LinkedHashMap<>();
        Path keystore = null;
        String password = env.get("EDGE_TLS_PASSWORD");
        String autoTarget = null;
        Integer autoDepth = null;
        Double autoCpu = null;
        for (int i = 0; i < args.length; i++) {
            String flag = args[i];
            switch (flag) {
                case "--name" -> name = value(args, ++i, flag);
                case "--port" -> port = parsePort(value(args, ++i, flag));
                case "--peer" -> addPeer(peers, value(args, ++i, flag));
                case "--tls" -> keystore = Path.of(value(args, ++i, flag));
                case "--password" -> password = value(args, ++i, flag);
                case "--auto-migrate" -> autoTarget = value(args, ++i, flag);
                case "--auto-depth" -> autoDepth = parseInt(value(args, ++i, flag), flag);
                case "--auto-cpu" -> autoCpu = parseFraction(value(args, ++i, flag), flag);
                default -> throw new IllegalArgumentException("unknown option " + flag);
            }
        }
        if (name == null) {
            throw new IllegalArgumentException("--name is required");
        }
        if (keystore != null && password == null) {
            throw new IllegalArgumentException("--tls needs --password or the EDGE_TLS_PASSWORD variable");
        }
        AutoMigrator.Policy auto = null;
        if (autoTarget == null) {
            if (autoDepth != null || autoCpu != null) {
                throw new IllegalArgumentException("--auto-depth and --auto-cpu need --auto-migrate <node>");
            }
        } else {
            if (!peers.containsKey(autoTarget)) {
                throw new IllegalArgumentException("--auto-migrate must name a --peer, got " + autoTarget);
            }
            auto = AutoMigrator.Policy.of(autoTarget, autoDepth == null ? DEFAULT_AUTO_DEPTH : autoDepth,
                autoCpu == null ? 0 : autoCpu);
        }
        return new NodeConfig(name, port, Collections.unmodifiableMap(peers), keystore, password, auto);
    }

    public static String usage() {
        return "usage: NodeMain --name <node> [--port <port>] [--peer <node>=<host>:<port> ...]"
            + " [--tls <keystore.p12> --password <password>]"
            + " [--auto-migrate <node> [--auto-depth <n>] [--auto-cpu <0..1>]]";
    }

    private static String value(String[] args, int index, String flag) {
        if (index >= args.length) {
            throw new IllegalArgumentException(flag + " needs a value");
        }
        return args[index];
    }

    private static int parseInt(String raw, String flag) {
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(flag + " needs a whole number, got " + raw);
        }
    }

    private static double parseFraction(String raw, String flag) {
        double value;
        try {
            value = Double.parseDouble(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(flag + " needs a number between 0 and 1, got " + raw);
        }
        if (!(value > 0 && value <= 1)) {
            throw new IllegalArgumentException(flag + " needs a number between 0 and 1, got " + raw);
        }
        return value;
    }

    private static int parsePort(String raw) {
        int port = Integer.parseInt(raw);
        if (port < 0 || port > 65_535) {
            throw new IllegalArgumentException("port out of range: " + raw);
        }
        return port;
    }

    private static void addPeer(Map<String, String> peers, String spec) {
        int eq = spec.indexOf('=');
        int colon = spec.lastIndexOf(':');
        if (eq <= 0 || colon <= eq + 1 || colon == spec.length() - 1) {
            throw new IllegalArgumentException("--peer must look like name=host:port, got " + spec);
        }
        peers.put(spec.substring(0, eq), spec.substring(eq + 1));
    }
}
