package com.northeastern.cs5010.baselines;

class LockedCounterServiceTest extends CounterServiceContract {

    @Override
    CounterService create(int keys, int shards) {
        return new LockedCounterService(keys, shards);
    }
}
