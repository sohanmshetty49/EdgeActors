package com.northeastern.cs5010.bench;

import com.northeastern.cs5010.actors.Supervision;

/**
 * The outcome of one failure-injection run.
 *
 * <p>Every job ends in exactly one bucket: processed, crashed (an injected
 * failure), or dead letter (sent to a worker that had been stopped). lost
 * counts jobs in none of them; it must be 0 for the runtime to be trusted.
 */
public record FaultResult(int workers, long jobs, double crashProbability, Supervision supervision,
                          long processed, long crashed, long deadLetters, long lost, long duplicates,
                          long recoveries, int workersAlive, long workersStopped, long elapsedNanos,
                          double recoveryP50Micros, double recoveryP99Micros) {

    public double throughputJobsPerSec() {
        return jobs / (elapsedNanos / 1e9);
    }

    /** Share of jobs that were processed successfully. */
    public double availability() {
        return jobs == 0 ? 1 : (double) processed / jobs;
    }
}
