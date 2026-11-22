package com.northeastern.cs5010.bench;

import java.util.SplittableRandom;

/**
 * Produces each client's operation stream from a WorkloadSpec.
 *
 * <p>Streams are generated before the clock starts, so random-number cost is
 * not measured, and they depend only on the seed and client number, so the
 * same spec produces the same operations on the Pi and on a laptop.
 */
public final class WorkloadGenerator {

    private final WorkloadSpec spec;

    public WorkloadGenerator(WorkloadSpec spec) {
        this.spec = spec;
    }

    public Operation[] opsFor(int client) {
        if (client < 0 || client >= spec.clients()) {
            throw new IllegalArgumentException("client out of range: " + client);
        }
        SplittableRandom random = new SplittableRandom(spec.seed() * 1_000_003L + client);
        Operation[] ops = new Operation[spec.opsPerClient()];
        for (int i = 0; i < ops.length; i++) {
            Operation.Kind kind = random.nextDouble() < spec.readRatio() ? Operation.Kind.READ : Operation.Kind.WRITE;
            ops[i] = new Operation(kind, pickKey(random));
        }
        return ops;
    }

    private int pickKey(SplittableRandom random) {
        int hot = spec.hotKeys();
        if (hot >= spec.keys() || random.nextDouble() < spec.hotTrafficFraction()) {
            return random.nextInt(hot);
        }
        return hot + random.nextInt(spec.keys() - hot);
    }
}
