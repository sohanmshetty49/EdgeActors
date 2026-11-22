package com.northeastern.cs5010.bench;

import static org.junit.jupiter.api.Assertions.*;

import com.northeastern.cs5010.baselines.CounterService;
import org.junit.jupiter.api.Test;

/** The runner itself: same workload on every model, correct totals, sane numbers. */
class BenchmarkRunnerTest {

    private static final WorkloadSpec SPEC = new WorkloadSpec(4, 2_000, 100, 0.2, 0.2, 0.8, 11);

    @Test
    void everyModelProcessesTheWorkloadCorrectly() throws Exception {
        for (String model : BenchmarkMain.MODELS) {
            try (CounterService service = BenchmarkMain.create(model, SPEC.keys(), 4)) {
                BenchmarkResult r = BenchmarkRunner.run(service, SPEC);
                assertEquals(model, r.model());
                assertEquals(SPEC.totalOps(), r.operations());
                assertTrue(r.reads() > 0 && r.writes() > 0);
                assertTrue(r.correct(), model + " lost or duplicated updates");
                assertTrue(r.throughputOpsPerSec() > 0);
                assertTrue(r.readP50Micros() <= r.readP95Micros() && r.readP95Micros() <= r.readP99Micros());
            }
        }
    }

    @Test
    void allModelsSeeTheSameNumberOfWrites() throws Exception {
        long writes = -1;
        for (String model : BenchmarkMain.MODELS) {
            try (CounterService service = BenchmarkMain.create(model, SPEC.keys(), 2)) {
                long w = BenchmarkRunner.run(service, SPEC).writes();
                if (writes >= 0) {
                    assertEquals(writes, w, "fair comparison needs identical workloads");
                }
                writes = w;
            }
        }
    }

    @Test
    void aFailingServiceIsReported() {
        CounterService broken = new CounterService() {
            @Override
            public String name() {
                return "broken";
            }

            @Override
            public void increment(int key) {
                throw new IllegalStateException("down");
            }

            @Override
            public long get(int key) {
                return 0;
            }

            @Override
            public long total() {
                return 0;
            }

            @Override
            public void close() {
            }
        };
        assertThrows(IllegalStateException.class, () -> BenchmarkRunner.run(broken, SPEC));
    }

    @Test
    void wrongTotalIsFlaggedAsIncorrect() {
        BenchmarkResult r = new BenchmarkResult("x", SPEC, 10, 5, 1_000_000, 9, 1, 2, 3);
        assertFalse(r.correct());
        assertEquals(15, r.operations());
        assertEquals(15_000, r.throughputOpsPerSec(), 1e-6);
    }
}
