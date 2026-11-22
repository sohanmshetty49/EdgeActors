package com.northeastern.cs5010.transport;

import static org.junit.jupiter.api.Assertions.*;

import com.northeastern.cs5010.transport.LocationRegistry.Status;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The atomic transitions the handoff protocol relies on. */
class LocationRegistryMigrationTest {

    private LocationRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new LocationRegistry();
    }

    @Test
    void registerAtKnownEpoch() {
        registry.register("a", "cloud", 7);
        assertEquals("cloud", registry.getLocation("a"));
        assertEquals(7, registry.getEpoch("a"));
        assertEquals(Status.ACTIVE, registry.getStatus("a"));
    }

    @Test
    void beginMigrationFromActive() {
        registry.register("a", "pi");
        assertTrue(registry.beginMigration("a", "pi"));
        assertEquals(Status.MIGRATING, registry.getStatus("a"));
        assertEquals(0, registry.getEpoch("a"));
    }

    @Test
    void beginMigrationRefusesWrongLocationOrStatusOrUnknown() {
        registry.register("a", "pi");
        assertFalse(registry.beginMigration("a", "cloud"));
        registry.setStatus("a", Status.PAUSED);
        assertFalse(registry.beginMigration("a", "pi"));
        assertFalse(registry.beginMigration("missing", "pi"));
    }

    @Test
    void onlyOneOfManyConcurrentMigrationsStarts() throws Exception {
        registry.register("a", "pi");
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch go = new CountDownLatch(1);
        AtomicInteger winners = new AtomicInteger();
        for (int i = 0; i < 8; i++) {
            pool.execute(() -> {
                try {
                    go.await();
                } catch (InterruptedException e) {
                    return;
                }
                if (registry.beginMigration("a", "pi")) {
                    winners.incrementAndGet();
                }
            });
        }
        go.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        assertEquals(1, winners.get());
    }

    @Test
    void commitMigrationMovesAndBumpsEpoch() {
        registry.register("a", "pi", 3);
        registry.beginMigration("a", "pi");
        assertTrue(registry.commitMigration("a", 3, "cloud"));
        assertEquals("cloud", registry.getLocation("a"));
        assertEquals(4, registry.getEpoch("a"));
        assertEquals(Status.ACTIVE, registry.getStatus("a"));
    }

    @Test
    void commitMigrationFencedByEpoch() {
        registry.register("a", "pi", 3);
        assertFalse(registry.commitMigration("a", 2, "cloud"));
        assertEquals("pi", registry.getLocation("a"));
        assertFalse(registry.commitMigration("missing", 0, "cloud"));
    }

    @Test
    void updateLocationKeepsStatus() {
        registry.register("a", "pi");
        registry.setStatus("a", Status.PAUSED);
        registry.updateLocation("a", "cloud");
        assertEquals(Status.PAUSED, registry.getStatus("a"));
        assertEquals(1, registry.getEpoch("a"));
    }

    @Test
    void changesToUnknownActorsAreIgnored() {
        registry.setStatus("missing", Status.ACTIVE);
        registry.updateLocation("missing", "cloud");
        assertNull(registry.getStatus("missing"));
        assertNull(registry.getLocation("missing"));
    }
}
