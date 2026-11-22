package com.northeastern.cs5010.bench;

import static org.junit.jupiter.api.Assertions.*;

import com.northeastern.cs5010.actors.Supervision;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The command-line runner and the report formats. */
class BenchmarkMainTest {

    @Test
    void runsAllModelsWritesCsvAndFaultTable() throws Exception {
        Path csv = Files.createTempFile("bench", ".csv");
        String out = run("--clients", "2", "--ops", "500", "--keys", "50", "--shards", "2",
            "--read-ratio", "0.2", "--hot", "0.2:0.8", "--seed", "7", "--warmup", "0", "--repeat", "1",
            "--csv", csv.toString(), "--faults", "--fault-jobs", "2000");
        assertTrue(out.contains("Environment: "), out);
        assertTrue(out.contains("80% of traffic on 20% of keys"), out);
        assertTrue(out.contains("| threads+locks |"), out);
        assertTrue(out.contains("| channels |"), out);
        assertTrue(out.contains("| actors |"), out);
        assertTrue(out.contains("Failure injection (2 supervised workers):"), out);
        assertTrue(out.contains("restart, 3 per 60 s"), out);
        List<String> lines = Files.readAllLines(csv);
        assertEquals(4, lines.size(), "header plus one row per model");
        assertTrue(lines.get(0).startsWith("model,clients"));
    }

    @Test
    void defaultsRunWithoutFaultTable() throws Exception {
        String out = run("--ops", "200", "--repeat", "1", "--warmup", "0");
        assertTrue(out.contains("| actors |"), out);
        assertTrue(out.contains("uniform keys"), out);
        assertFalse(out.contains("Failure injection"), out);
    }

    @Test
    void badArgumentsPrintUsage() throws Exception {
        for (String[] args : new String[][] {
            {"--bogus"}, {"--ops"}, {"--hot", "0.5"}, {"--repeat", "0"}, {"--shards", "0"},
            {"--warmup", "-1"}, {"--fault-jobs", "-1"}, {"--clients", "x"},
        }) {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            assertEquals(2, BenchmarkMain.run(args, new PrintStream(bytes, true, StandardCharsets.UTF_8)),
                String.join(" ", args));
            assertTrue(bytes.toString(StandardCharsets.UTF_8).contains("usage:"));
        }
    }

    @Test
    void unknownModelIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> BenchmarkMain.create("goroutines", 10, 1));
    }

    @Test
    void medianPicksTheMiddleRun() {
        WorkloadSpec spec = WorkloadSpec.uniform(1, 1, 1, 0, 1);
        BenchmarkResult slow = new BenchmarkResult("m", spec, 1, 0, 3_000, 1, 0, 0, 0);
        BenchmarkResult mid = new BenchmarkResult("m", spec, 1, 0, 2_000, 1, 0, 0, 0);
        BenchmarkResult fast = new BenchmarkResult("m", spec, 1, 0, 1_000, 1, 0, 0, 0);
        assertSame(mid, BenchmarkMain.median(List.of(fast, slow, mid)));
    }

    @Test
    void reportsShowEveryPolicyAndIncorrectRuns() {
        WorkloadSpec spec = WorkloadSpec.uniform(1, 1, 1, 0, 1);
        String table = BenchmarkReport.markdown(List.of(new BenchmarkResult("x", spec, 2, 0, 1_000, 1, 0, 0, 0)));
        assertTrue(table.contains("| NO |"), table);
        String faults = BenchmarkReport.faultMarkdown(List.of(
            fault(Supervision.stop()), fault(Supervision.escalate()),
            fault(Supervision.restart(5, Duration.ofSeconds(10)))));
        assertTrue(faults.contains("| stop |") && faults.contains("| escalate |")
            && faults.contains("restart, 5 per 10 s"), faults);
    }

    private static FaultResult fault(Supervision s) {
        return new FaultResult(1, 1, 0, s, 1, 0, 0, 0, 0, 0, 1, 0, 1_000, 0, 0);
    }

    private static String run(String... args) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        assertEquals(0, BenchmarkMain.run(args, new PrintStream(bytes, true, StandardCharsets.UTF_8)));
        return bytes.toString(StandardCharsets.UTF_8);
    }
}
