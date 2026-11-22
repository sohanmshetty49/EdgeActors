package com.northeastern.cs5010.migration;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Per-actor mailboxes: ordering, isolation, idleness, lifecycle. */
class LocalActorHostTest {

    private LocalActorHost host;

    @BeforeEach
    void setUp() {
        host = new LocalActorHost(LocalCluster.factories());
    }

    @AfterEach
    void tearDown() {
        for (String id : new String[] {"c", "d", "s", "boom"}) {
            host.stop(id);
        }
    }

    @Test
    void deliversInOrderOnTheActorsOwnThread() throws Exception {
        host.install("c", CounterActor.TYPE, null);
        for (int i = 1; i <= 1_000; i++) {
            host.deliver("c", Integer.toString(i));
        }
        assertTrue(host.awaitIdle("c", Duration.ofSeconds(5)));
        assertEquals("1000,500500,1000,0", host.snapshot("c"));
    }

    @Test
    void installRestoresState() {
        host.install("c", CounterActor.TYPE, "7,28,7,0");
        assertEquals(7L, (long) host.inspect("c", a -> ((CounterActor) a).count()));
        assertEquals(CounterActor.TYPE, host.typeOf("c"));
        assertTrue(host.hosts("c"));
    }

    @Test
    void installRejectsDuplicatesAndUnknownTypes() {
        host.install("c", CounterActor.TYPE, null);
        assertThrows(IllegalStateException.class, () -> host.install("c", CounterActor.TYPE, null));
        assertThrows(IllegalArgumentException.class, () -> host.install("d", "unknown", null));
        assertFalse(host.hosts("d"));
    }

    @Test
    void unknownActorsAreReported() {
        assertNull(host.typeOf("missing"));
        assertFalse(host.hosts("missing"));
        assertThrows(IllegalArgumentException.class, () -> host.deliver("missing", "x"));
        assertThrows(IllegalArgumentException.class, () -> host.snapshot("missing"));
        assertTrue(host.canInstall(CounterActor.TYPE));
        assertFalse(host.canInstall("unknown"));
        host.stop("missing"); // no effect
    }

    @Test
    void awaitIdleTimesOutOnBusyActor() throws Exception {
        host.install("s", MisbehavingActors.SLOW, null);
        host.deliver("s", "work");
        host.deliver("s", "work");
        assertFalse(host.awaitIdle("s", Duration.ofMillis(50)));
        assertTrue(host.awaitIdle("s", Duration.ofSeconds(3)));
    }

    @Test
    void failingMessageDoesNotKillTheMailbox() throws Exception {
        Map<String, Supplier<? extends MigratableActor>> f = Map.of("boom", ExplodingActor::new);
        LocalActorHost h = new LocalActorHost(f);
        h.install("boom", "boom", null);
        h.deliver("boom", "explode");
        h.deliver("boom", "ok");
        assertTrue(h.awaitIdle("boom", Duration.ofSeconds(3)));
        assertEquals("1", h.snapshot("boom"));
        h.stop("boom");
    }

    @Test
    void failingQueryIsReported() {
        host.install("c", CounterActor.TYPE, null);
        assertThrows(IllegalStateException.class, () -> host.inspect("c", a -> {
            throw new IllegalArgumentException("bad query");
        }));
    }

    @Test
    void stopRemovesTheActor() {
        host.install("c", CounterActor.TYPE, null);
        host.stop("c");
        assertFalse(host.hosts("c"));
        host.install("c", CounterActor.TYPE, null); // the id can be reused
        assertTrue(host.hosts("c"));
    }

    /** Counts only the messages it handled without throwing. */
    static final class ExplodingActor implements MigratableActor {
        int ok;

        @Override
        public void onMessage(String payload) {
            if (payload.equals("explode")) {
                throw new IllegalStateException("boom");
            }
            ok++;
        }

        @Override
        public String snapshot() {
            return Integer.toString(ok);
        }

        @Override
        public void restore(String state) {
            ok = Integer.parseInt(state);
        }
    }
}
