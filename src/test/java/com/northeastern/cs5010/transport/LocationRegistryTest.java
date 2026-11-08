package com.northeastern.cs5010.transport;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for LocationRegistry.
 */
public class LocationRegistryTest {
    private LocationRegistry registry;

    @BeforeEach
    public void setup() {
        registry = new LocationRegistry();
    }

    @Test
    public void testRegister() {
        registry.register("actor-1", "node-1");
        assertEquals("node-1", registry.getLocation("actor-1"));
    }

    @Test
    public void testEpochTracking() {
        registry.register("actor-1", "node-1");
        assertEquals(0, registry.getEpoch("actor-1"));
    }

    @Test
    public void testUpdateLocation() {
        registry.register("actor-1", "node-1");
        registry.updateLocation("actor-1", "node-2");

        assertEquals("node-2", registry.getLocation("actor-1"));
        assertEquals(1, registry.getEpoch("actor-1"));
    }

    @Test
    public void testSetStatus() {
        registry.register("actor-1", "node-1");
        registry.setStatus("actor-1", LocationRegistry.Status.MIGRATING);

        assertEquals(LocationRegistry.Status.MIGRATING, registry.getStatus("actor-1"));
    }

    @Test
    public void testNotFound() {
        assertNull(registry.getLocation("nonexistent"));
        assertEquals(-1, registry.getEpoch("nonexistent"));
    }

    @Test
    public void testRemove() {
        registry.register("actor-1", "node-1");
        registry.remove("actor-1");

        assertNull(registry.getLocation("actor-1"));
    }
}
