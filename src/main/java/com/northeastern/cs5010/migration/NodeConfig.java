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
 * </pre>
 */
public record NodeConfig(String name, int port, Map<String, String> peers, Path keystore, String keystorePassword) {

    public static final int DEFAULT_PORT = 9100;

    public boolean tls() {
        return keystore != null;
    }

    public static NodeConfig parse(String[] args, Map<String, String> env) {
        String name = null;
        int port = DEFAULT_PORT;
        Map<String, String> peers = new LinkedHashMap<>();
        Path keystore = null;
        String password = env.get("EDGE_TLS_PASSWORD");
        for (int i = 0; i < args.length; i++) {
            String flag = args[i];
            switch (flag) {
                case "--name" -> name = value(args, ++i, flag);
                case "--port" -> port = parsePort(value(args, ++i, flag));
                case "--peer" -> addPeer(peers, value(args, ++i, flag));
                case "--tls" -> keystore = Path.of(value(args, ++i, flag));
                case "--password" -> password = value(args, ++i, flag);
                default -> throw new IllegalArgumentException("unknown option " + flag);
            }
        }
        if (name == null) {
            throw new IllegalArgumentException("--name is required");
        }
        if (keystore != null && password == null) {
            throw new IllegalArgumentException("--tls needs --password or the EDGE_TLS_PASSWORD variable");
        }
        return new NodeConfig(name, port, Collections.unmodifiableMap(peers), keystore, password);
    }

    public static String usage() {
        return "usage: NodeMain --name <node> [--port <port>] [--peer <node>=<host>:<port> ...]"
            + " [--tls <keystore.p12> --password <password>]";
    }

    private static String value(String[] args, int index, String flag) {
        if (index >= args.length) {
            throw new IllegalArgumentException(flag + " needs a value");
        }
        return args[index];
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
