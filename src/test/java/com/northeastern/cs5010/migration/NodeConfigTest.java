package com.northeastern.cs5010.migration;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;

class NodeConfigTest {

    @Test
    void parsesEveryOption() {
        NodeConfig c = NodeConfig.parse(new String[] {
            "--name", "pi", "--port", "9200",
            "--peer", "mac=192.168.1.20:9100", "--peer", "cloud=10.0.0.4:9100",
            "--tls", "certs/pi.p12", "--password", "secret"}, Map.of());
        assertEquals("pi", c.name());
        assertEquals(9200, c.port());
        assertEquals(Map.of("mac", "192.168.1.20:9100", "cloud", "10.0.0.4:9100"), c.peers());
        assertEquals(Path.of("certs/pi.p12"), c.keystore());
        assertEquals("secret", c.keystorePassword());
        assertTrue(c.tls());
    }

    @Test
    void defaultsAndEnvironmentPassword() {
        NodeConfig c = NodeConfig.parse(new String[] {"--name", "pi", "--tls", "pi.p12"},
            Map.of("EDGE_TLS_PASSWORD", "from-env"));
        assertEquals(NodeConfig.DEFAULT_PORT, c.port());
        assertEquals(Map.of(), c.peers());
        assertEquals("from-env", c.keystorePassword());
        assertFalse(NodeConfig.parse(new String[] {"--name", "pi"}, Map.of()).tls());
    }

    @Test
    void rejectsBadInput() {
        String[][] bad = {
            {},
            {"--port", "9100"},
            {"--name"},
            {"--name", "pi", "--bogus"},
            {"--name", "pi", "--port", "70000"},
            {"--name", "pi", "--port", "-1"},
            {"--name", "pi", "--port", "abc"},
            {"--name", "pi", "--peer", "mac"},
            {"--name", "pi", "--peer", "=host:1"},
            {"--name", "pi", "--peer", "mac=host"},
            {"--name", "pi", "--peer", "mac=host:"},
            {"--name", "pi", "--tls", "pi.p12"},
        };
        for (String[] args : bad) {
            assertThrows(IllegalArgumentException.class, () -> NodeConfig.parse(args, Map.of()),
                String.join(" ", args));
        }
        assertTrue(NodeConfig.usage().contains("--name"));
    }
}
