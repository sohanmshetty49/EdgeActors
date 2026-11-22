package com.northeastern.cs5010.actors;

import java.time.Duration;
import java.util.Objects;

/**
 * How a supervisor handles the failure of one child: restart it with fresh
 * state, stop it, or escalate the failure to the supervisor's own supervisor.
 *
 * <p>A RESTART directive comes with a restart budget: at most maxRestarts
 * restarts within any sliding window of the given length. When a failure
 * would exceed the budget the actor is not restarted again; the failure is
 * escalated instead, so a crash loop cannot run forever.
 */
public record Supervision(Directive directive, int maxRestarts, Duration window) {

    public enum Directive {
        RESTART, STOP, ESCALATE
    }

    /** Restart up to 3 times per minute, then escalate. Used when no policy is given. */
    public static final Supervision DEFAULT = restart(3, Duration.ofMinutes(1));

    public Supervision {
        Objects.requireNonNull(directive, "directive");
        Objects.requireNonNull(window, "window");
        if (maxRestarts < 0) {
            throw new IllegalArgumentException("maxRestarts must not be negative");
        }
        if (window.isZero() || window.isNegative()) {
            throw new IllegalArgumentException("window must be positive");
        }
    }

    public static Supervision restart(int maxRestarts, Duration window) {
        return new Supervision(Directive.RESTART, maxRestarts, window);
    }

    public static Supervision stop() {
        return new Supervision(Directive.STOP, 0, Duration.ofSeconds(1));
    }

    public static Supervision escalate() {
        return new Supervision(Directive.ESCALATE, 0, Duration.ofSeconds(1));
    }
}
