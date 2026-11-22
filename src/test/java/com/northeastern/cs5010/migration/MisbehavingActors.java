package com.northeastern.cs5010.migration;

/** Misbehaving actors used to drive the protocol's failure paths. */
final class MisbehavingActors {

    static final String SLOW = "slow";
    static final String NO_SNAPSHOT = "no-snapshot";
    static final String NO_RESTORE = "no-restore";

    private MisbehavingActors() {
    }

    /** Takes 200 ms per message, so it is never idle within a short freeze timeout. */
    static final class Slow implements MigratableActor {
        long handled;

        @Override
        public void onMessage(String payload) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            handled++;
        }

        @Override
        public String snapshot() {
            return Long.toString(handled);
        }

        @Override
        public void restore(String state) {
            handled = Long.parseLong(state);
        }
    }

    /** Cannot capture its state. */
    static final class NoSnapshot implements MigratableActor {
        @Override
        public void onMessage(String payload) {
        }

        @Override
        public String snapshot() {
            throw new IllegalStateException("snapshot not supported");
        }

        @Override
        public void restore(String state) {
        }
    }

    /** Snapshots fine but cannot be rebuilt, so installing it on a destination fails. */
    static final class NoRestore implements MigratableActor {
        @Override
        public void onMessage(String payload) {
        }

        @Override
        public String snapshot() {
            return "anything";
        }

        @Override
        public void restore(String state) {
            throw new IllegalStateException("restore not supported");
        }
    }
}
