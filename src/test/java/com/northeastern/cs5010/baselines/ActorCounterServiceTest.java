package com.northeastern.cs5010.baselines;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class ActorCounterServiceTest extends CounterServiceContract {

    @Override
    CounterService create(int keys, int shards) {
        return new ActorCounterService(keys, shards);
    }

    @Test
    void usingAClosedServiceFails() {
        service.close();
        assertThrows(IllegalStateException.class, () -> service.increment(1));
    }

    @Test
    void readsSeeEarlierWritesFromTheSameClient() throws Exception {
        for (int i = 0; i < 100; i++) {
            service.increment(7);
        }
        assertEquals(100, service.get(7)); // same shard, FIFO: the read queues behind the writes
    }
}
