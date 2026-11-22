package com.northeastern.cs5010.bench;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Generated workloads must be reproducible and match the requested mix and skew. */
class WorkloadGeneratorTest {

    @Test
    void sameSeedGivesTheSameOperations() {
        WorkloadSpec spec = WorkloadSpec.uniform(2, 1_000, 100, 0.3, 7);
        assertArrayEquals(new WorkloadGenerator(spec).opsFor(1), new WorkloadGenerator(spec).opsFor(1));
    }

    @Test
    void differentClientsGetDifferentStreams() {
        WorkloadGenerator g = new WorkloadGenerator(WorkloadSpec.uniform(2, 1_000, 100, 0.3, 7));
        assertFalse(java.util.Arrays.equals(g.opsFor(0), g.opsFor(1)));
    }

    @Test
    void readRatioAndKeyRangeAreRespected() {
        WorkloadSpec spec = WorkloadSpec.uniform(1, 100_000, 50, 0.25, 1);
        int reads = 0;
        for (Operation op : new WorkloadGenerator(spec).opsFor(0)) {
            assertTrue(op.key() >= 0 && op.key() < 50);
            if (op.kind() == Operation.Kind.READ) {
                reads++;
            }
        }
        assertEquals(0.25, reads / 100_000.0, 0.01);
    }

    @Test
    void hotSpotSkewSendsMostTrafficToFewKeys() {
        WorkloadSpec spec = new WorkloadSpec(1, 100_000, 100, 0, 0.2, 0.8, 3);
        assertEquals(20, spec.hotKeys());
        int hot = 0;
        for (Operation op : new WorkloadGenerator(spec).opsFor(0)) {
            if (op.key() < 20) {
                hot++;
            }
        }
        assertEquals(0.8, hot / 100_000.0, 0.01);
    }

    @Test
    void specValidation() {
        assertThrows(IllegalArgumentException.class, () -> WorkloadSpec.uniform(0, 1, 1, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> WorkloadSpec.uniform(1, 0, 1, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> WorkloadSpec.uniform(1, 1, 0, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> WorkloadSpec.uniform(1, 1, 1, 1.5, 1));
        assertThrows(IllegalArgumentException.class, () -> WorkloadSpec.uniform(1, 1, 1, -0.1, 1));
        assertThrows(IllegalArgumentException.class, () -> new WorkloadSpec(1, 1, 1, 0, 0, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new WorkloadSpec(1, 1, 1, 0, 1.5, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new WorkloadSpec(1, 1, 1, 0, 1, 2, 1));
        assertThrows(IllegalArgumentException.class,
            () -> new WorkloadGenerator(WorkloadSpec.uniform(2, 1, 1, 0, 1)).opsFor(2));
        assertThrows(IllegalArgumentException.class,
            () -> new WorkloadGenerator(WorkloadSpec.uniform(2, 1, 1, 0, 1)).opsFor(-1));
        assertEquals(6, WorkloadSpec.uniform(2, 3, 1, 0, 1).totalOps());
        assertEquals(1, new WorkloadSpec(1, 1, 3, 0, 0.01, 1, 1).hotKeys(), "never fewer than one hot key");
    }
}
