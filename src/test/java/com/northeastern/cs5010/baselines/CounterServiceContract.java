package com.northeastern.cs5010.baselines;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests every CounterService must pass. Each concurrency model gets a small
 * subclass, so all three are checked against exactly the same behaviour.
 */
abstract class CounterServiceContract {

    static final int KEYS = 64;
    protected CounterService service;

    abstract CounterService create(int keys, int shards);

    @BeforeEach
    void setUp() {
        service = create(KEYS, 4);
    }

    @AfterEach
    void tearDown() {
        service.close();
    }

    @Test
    void countsSingleThreaded() throws Exception {
        service.increment(3);
        service.increment(3);
        service.increment(10);
        assertEquals(2, service.get(3));
        assertEquals(1, service.get(10));
        assertEquals(0, service.get(11));
        assertEquals(3, service.total());
    }

    @Test
    void noLostOrDuplicatedUpdatesUnderContention() throws Exception {
        int threads = 8;
        int perThread = 5_000;
        List<Thread> workers = new ArrayList<>();
        List<Throwable> errors = new java.util.concurrent.CopyOnWriteArrayList<>();
        for (int t = 0; t < threads; t++) {
            int offset = t;
            Thread w = new Thread(() -> {
                try {
                    for (int i = 0; i < perThread; i++) {
                        service.increment((i + offset) % KEYS);
                    }
                } catch (Throwable e) {
                    errors.add(e);
                }
            });
            workers.add(w);
            w.start();
        }
        for (Thread w : workers) {
            w.join();
        }
        assertTrue(errors.isEmpty(), errors.toString());
        assertEquals((long) threads * perThread, service.total());
    }

    @Test
    void rejectsKeysOutOfRange() {
        assertThrows(IllegalArgumentException.class, () -> service.increment(-1));
        assertThrows(IllegalArgumentException.class, () -> service.get(KEYS));
    }

    @Test
    void rejectsBadConfiguration() {
        assertThrows(IllegalArgumentException.class, () -> create(0, 1));
        assertThrows(IllegalArgumentException.class, () -> create(1, 0));
    }

    @Test
    void closeIsIdempotentAndHasAName() {
        assertFalse(service.name().isBlank());
        service.close();
        service.close();
    }
}
