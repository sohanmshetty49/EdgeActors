package com.northeastern.cs5010.bench;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class LatencyRecorderTest {

    @Test
    void nearestRankPercentiles() {
        LatencyRecorder r = new LatencyRecorder();
        for (int i = 100; i >= 1; i--) {
            r.record(i * 1_000L); // 1..100 microseconds, inserted in reverse
        }
        assertEquals(100, r.count());
        assertEquals(50.0, r.percentileMicros(50), 1e-9);
        assertEquals(95.0, r.percentileMicros(95), 1e-9);
        assertEquals(99.0, r.percentileMicros(99), 1e-9);
        assertEquals(100.0, r.percentileMicros(100), 1e-9);
        assertEquals(1.0, r.percentileMicros(0.5), 1e-9);
    }

    @Test
    void growsAndMerges() {
        LatencyRecorder a = new LatencyRecorder();
        LatencyRecorder b = new LatencyRecorder();
        for (int i = 0; i < 5_000; i++) {
            a.record(1_000);
            b.record(3_000);
        }
        a.addAll(b);
        assertEquals(10_000, a.count());
        assertEquals(1.0, a.percentileMicros(50), 1e-9);
        assertEquals(3.0, a.percentileMicros(51), 1e-9);
    }

    @Test
    void emptyAndInvalid() {
        LatencyRecorder r = new LatencyRecorder();
        assertEquals(0.0, r.percentileMicros(99), 1e-9);
        assertThrows(IllegalArgumentException.class, () -> r.percentileMicros(0));
        assertThrows(IllegalArgumentException.class, () -> r.percentileMicros(101));
    }
}
