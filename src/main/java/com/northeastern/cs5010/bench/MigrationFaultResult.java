package com.northeastern.cs5010.bench;

import com.northeastern.cs5010.migration.MigrationManager;
import java.util.List;

/**
 * Totals over all randomized migration runs under network faults.
 *
 * @param settings what was injected
 * @param sent numbered messages the clients tried to send
 * @param accepted sends that succeeded
 * @param reported sends that failed with an error the client saw (allowed during a partition)
 * @param received distinct messages that reached the actor
 * @param lost accepted messages that never reached the actor (I1, must be 0)
 * @param duplicates messages that reached the actor twice (I2, must be 0)
 * @param phantoms messages reported as failed that arrived anyway (I2, must be 0)
 * @param outOfOrder messages that arrived after a higher number (must be 0)
 * @param locationViolations moments with two active copies, or runs ending with none (I3, must be 0)
 * @param committed migrations that committed
 * @param aborted migrations that aborted and left the actor where it was
 * @param inDoubtResolved migrations that were IN_DOUBT and later settled with resolve
 * @param unresolved migrations still IN_DOUBT at the end of a run (must be 0)
 * @param partitions network partitions injected
 * @param droppedProtocol protocol messages lost on purpose
 * @param delayedSends sends held back by a random delay
 * @param refusedSends sends refused during a partition
 * @param managerDropped messages the migration managers had to drop and log
 * @param elapsedMillis wall time for all runs
 */
public record MigrationFaultResult(MigrationFaultHarness.Settings settings, long sent, long accepted, long reported,
                                   long received, long lost, long duplicates, long phantoms, long outOfOrder,
                                   long locationViolations, long committed, long aborted, long inDoubtResolved,
                                   long unresolved, long partitions, long droppedProtocol, long delayedSends,
                                   long refusedSends, long managerDropped, long elapsedMillis) {

    /** True when every run kept all three invariants and every migration was decided. */
    public boolean invariantsHold() {
        return lost == 0 && duplicates == 0 && phantoms == 0 && outOfOrder == 0 && locationViolations == 0
            && unresolved == 0;
    }

    /** Adds up runs. */
    static final class Builder {
        private final MigrationFaultHarness.Settings settings;
        private long sent;
        private long accepted;
        private long reported;
        private long received;
        private long lost;
        private long duplicates;
        private long phantoms;
        private long outOfOrder;
        private long violations;
        private long committed;
        private long aborted;
        private long resolved;
        private long unresolved;
        private long partitions;
        private long dropped;
        private long delayed;
        private long refused;
        private long managerDropped;

        Builder(MigrationFaultHarness.Settings settings) {
            this.settings = settings;
        }

        void add(long sent, long accepted, long reported, long received, long lost, long duplicates, long phantoms,
                 long outOfOrder, long violations, List<MigrationManager.Result> results, long resolved,
                 long unresolved, long partitions, long dropped, long delayed, long refused, long managerDropped) {
            this.sent += sent;
            this.accepted += accepted;
            this.reported += reported;
            this.received += received;
            this.lost += lost;
            this.duplicates += duplicates;
            this.phantoms += phantoms;
            this.outOfOrder += outOfOrder;
            this.violations += violations;
            for (MigrationManager.Result r : results) {
                switch (r.outcome()) {
                    case COMMITTED -> committed++;
                    case ABORTED -> aborted++;
                    case IN_DOUBT -> { } // counted through unresolved
                }
            }
            this.resolved += resolved;
            this.unresolved += unresolved;
            this.partitions += partitions;
            this.dropped += dropped;
            this.delayed += delayed;
            this.refused += refused;
            this.managerDropped += managerDropped;
        }

        MigrationFaultResult build(long elapsedMillis) {
            return new MigrationFaultResult(settings, sent, accepted, reported, received, lost, duplicates, phantoms,
                outOfOrder, violations, committed, aborted, resolved, unresolved, partitions, dropped, delayed,
                refused, managerDropped, elapsedMillis);
        }
    }
}
