package com.northeastern.cs5010.migration;

import com.northeastern.cs5010.transport.FramedTransport;
import com.northeastern.cs5010.transport.LocationRegistry;
import com.northeastern.cs5010.transport.TCPTransport;
import com.northeastern.cs5010.transport.TlsContexts;
import com.northeastern.cs5010.transport.TlsTransport;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Runs one EdgeActors node from the command line, on the Pi or on a laptop.
 *
 * <pre>
 * java -cp target/edge-actors-0.1.0.jar com.northeastern.cs5010.migration.NodeMain \
 *      --name pi --port 9100 --peer mac=192.168.1.20:9100
 * </pre>
 */
public final class NodeMain {

    private NodeMain() {
    }

    public static void main(String[] args) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        System.exit(run(args, System.getenv(), in, System.out));
    }

    /** Runs the node until "quit" or end of input. Returns the process exit code. */
    static int run(String[] args, Map<String, String> env, BufferedReader in, PrintStream out) throws IOException {
        NodeConfig config;
        try {
            config = NodeConfig.parse(args, env);
        } catch (IllegalArgumentException e) {
            out.println(e.getMessage());
            out.println(NodeConfig.usage());
            return 2;
        }
        FramedTransport transport;
        try {
            transport = createTransport(config);
        } catch (GeneralSecurityException | IOException e) {
            out.println("could not load TLS keystore: " + e.getMessage());
            return 1;
        }
        transport.start();
        LocalActorHost host = defaultHost();
        LocationRegistry registry = new LocationRegistry();
        try (MigrationManager manager = new MigrationManager(host, registry, transport, config.peers(),
                 MigrationManager.Config.defaults());
             AutoMigrator auto = config.autoMigration()
                 ? new AutoMigrator(manager, host, config.autoMigrate()) : null) {
            NodeShell shell = new NodeShell(manager, host, registry, transport);
            out.println("node " + config.name() + " listening on port " + config.port()
                + (config.tls() ? " with mutual TLS" : " over plain TCP") + ", peers " + config.peers());
            if (auto != null) {
                auto.start();
                out.println("auto-migration on: " + auto.policy().describe());
            }
            out.println("type help for commands");
            String line;
            while ((line = in.readLine()) != null) {
                String command = line.trim();
                if (command.equals("quit") || command.equals("exit")) {
                    break;
                }
                String result = shell.execute(line);
                if (!result.isEmpty()) {
                    out.println(result);
                }
            }
        } finally {
            transport.close();
        }
        return 0;
    }

    static FramedTransport createTransport(NodeConfig config) throws IOException, GeneralSecurityException {
        if (!config.tls()) {
            return new TCPTransport(config.name(), config.port());
        }
        return new TlsTransport(config.name(), config.port(),
            TlsContexts.fromKeystore(config.keystore(), config.keystorePassword().toCharArray()));
    }

    /** The actor types every node can create or receive. */
    static LocalActorHost defaultHost() {
        Map<String, Supplier<? extends MigratableActor>> factories = Map.of(CounterActor.TYPE, CounterActor::new);
        return new LocalActorHost(factories);
    }
}
