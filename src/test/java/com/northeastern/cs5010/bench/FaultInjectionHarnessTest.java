package com.northeastern.cs5010.bench;

import static org.junit.jupiter.api.Assertions.*;

import com.northeastern.cs5010.actors.Supervision;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Failure injection against supervised workers. The key property in every
 * scenario: no job is ever lost without being accounted for.
 */
class FaultInjectionHarnessTest {

    private static final Supervision GENEROUS = Supervision.restart(1_000_000, Duration.ofMinutes(1));

    @Test
    void withoutFaultsEveryJobIsProcessed() throws Exception {
        FaultResult r = FaultInjectionHarness.run(4, 20_000, FaultPlan.none(), GENEROUS, Duration.ofSeconds(30));
        assertEquals(20_000, r.processed());
        assertEquals(0, r.crashed());
        assertEquals(0, r.lost());
        assertEquals(0, r.duplicates());
        assertEquals(4, r.workersAlive());
        assertEquals(1.0, r.availability(), 1e-9);
        assertTrue(r.throughputJobsPerSec() > 0);
    }

    @Test
    void supervisionRecoversFromInjectedCrashes() throws Exception {
        FaultPlan plan = new FaultPlan(0.02, 5);
        FaultResult r = FaultInjectionHarness.run(4, 20_000, plan, GENEROUS, Duration.ofSeconds(30));
        long expectedCrashes = java.util.stream.LongStream.range(0, 20_000).filter(plan::crashes).count();
        assertEquals(expectedCrashes, r.crashed(), "exactly the planned jobs crashed");
        assertEquals(20_000 - expectedCrashes, r.processed(), "only the crashing jobs are lost to the failure");
        assertEquals(0, r.lost(), "nothing disappears unaccounted");
        assertEquals(0, r.duplicates());
        assertEquals(0, r.deadLetters());
        assertEquals(4, r.workersAlive(), "every worker was restarted");
        assertTrue(r.recoveries() > 0);
        assertTrue(r.recoveryP50Micros() <= r.recoveryP99Micros());
    }

    @Test
    void exhaustedRestartBudgetStopsWorkersAndReportsDeadLetters() throws Exception {
        FaultResult r = FaultInjectionHarness.run(2, 5_000, new FaultPlan(0.2, 9),
            Supervision.restart(2, Duration.ofMinutes(1)), Duration.ofSeconds(30));
        assertEquals(0, r.workersAlive());
        assertEquals(2, r.workersStopped());
        assertTrue(r.deadLetters() > 0);
        assertEquals(0, r.lost(), "messages to stopped workers are counted, not dropped");
        assertTrue(r.availability() < 0.5);
    }

    @Test
    void stopPolicyStopsAWorkerOnItsFirstCrash() throws Exception {
        FaultResult r = FaultInjectionHarness.run(1, 1_000, new FaultPlan(1.0, 1), Supervision.stop(),
            Duration.ofSeconds(30));
        assertEquals(1, r.crashed());
        assertEquals(999, r.deadLetters());
        assertEquals(0, r.workersAlive());
        assertEquals(0, r.lost());
    }

    @Test
    void rejectsBadArguments() {
        assertThrows(IllegalArgumentException.class,
            () -> FaultInjectionHarness.run(0, 10, FaultPlan.none(), GENEROUS, Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class,
            () -> FaultInjectionHarness.run(1, -1, FaultPlan.none(), GENEROUS, Duration.ofSeconds(1)));
    }

    @Test
    void zeroJobsIsFullyAvailable() throws Exception {
        FaultResult r = FaultInjectionHarness.run(1, 0, FaultPlan.none(), GENEROUS, Duration.ofSeconds(1));
        assertEquals(1.0, r.availability(), 1e-9);
    }

    @Test
    void faultPlanIsDeterministicAndMatchesItsRate() {
        FaultPlan plan = new FaultPlan(0.1, 42);
        long hits = java.util.stream.LongStream.range(0, 100_000).filter(plan::crashes).count();
        assertEquals(0.1, hits / 100_000.0, 0.01);
        for (long id = 0; id < 1_000; id++) {
            assertEquals(plan.crashes(id), new FaultPlan(0.1, 42).crashes(id));
        }
        assertFalse(FaultPlan.none().crashes(5));
        assertTrue(new FaultPlan(1.0, 3).crashes(5));
        assertThrows(IllegalArgumentException.class, () -> new FaultPlan(-0.1, 1));
        assertThrows(IllegalArgumentException.class, () -> new FaultPlan(1.1, 1));
    }
}
