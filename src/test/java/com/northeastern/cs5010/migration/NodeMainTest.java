package com.northeastern.cs5010.migration;

import static org.junit.jupiter.api.Assertions.*;

import com.northeastern.cs5010.transport.TlsFixtures;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Runs whole nodes the way the Pi does, with scripted input. */
class NodeMainTest {

    @Test
    void runsCommandsUntilQuit() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int code = run(new String[] {"--name", "solo", "--port", "0"},
            "help\nspawn c\nsend c 3\nstatus c\n\nquit\nsend c 3\n", out, Map.of());
        String text = out.toString(StandardCharsets.UTF_8);
        assertEquals(0, code);
        assertTrue(text.contains("node solo listening on port 0 over plain TCP"), text);
        assertTrue(text.contains("count=3 sum=6 outOfOrder=0"), text);
        assertFalse(text.contains("(4..6)"), "commands after quit are not run");
    }

    @Test
    void runsOverTlsUntilEndOfInput() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String keystore = TlsFixtures.dir().resolve("cluster.p12").toString();
        int code = run(new String[] {"--name", "secure", "--port", "0", "--tls", keystore},
            "stats\n", out, Map.of("EDGE_TLS_PASSWORD", TlsFixtures.PASSWORD));
        String text = out.toString(StandardCharsets.UTF_8);
        assertEquals(0, code);
        assertTrue(text.contains("with mutual TLS"), text);
        assertTrue(text.contains("node=secure framesSent=0"), text);
    }

    @Test
    void exitAlsoStopsTheNode() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertEquals(0, run(new String[] {"--name", "solo", "--port", "0"}, "exit\nhelp\n", out, Map.of()));
        assertFalse(out.toString(StandardCharsets.UTF_8).contains("spawn <actor>"));
    }

    @Test
    void badArgumentsPrintUsage() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertEquals(2, run(new String[] {"--port", "1"}, "", out, Map.of()));
        assertTrue(out.toString(StandardCharsets.UTF_8).contains("usage:"));
    }

    @Test
    void unreadableKeystoreFails() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertEquals(1, run(new String[] {"--name", "x", "--tls", "/no/such/file.p12", "--password", "p"},
            "", out, Map.of()));
        assertTrue(out.toString(StandardCharsets.UTF_8).contains("could not load TLS keystore"));
    }

    @Test
    void defaultHostKnowsTheCounter() {
        assertTrue(NodeMain.defaultHost().canInstall(CounterActor.TYPE));
    }

    private static int run(String[] args, String input, ByteArrayOutputStream out, Map<String, String> env)
            throws Exception {
        try (PrintStream ps = new PrintStream(out, true, StandardCharsets.UTF_8)) {
            return NodeMain.run(args, env, new BufferedReader(new StringReader(input)), ps);
        }
    }
}
