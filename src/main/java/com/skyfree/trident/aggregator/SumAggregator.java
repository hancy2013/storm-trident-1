package com.skyfree.trident.aggregator;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/8 18:49
 */
public class SumAggregator extends BaseAggregator<SumAggregator.State> {
    public State init(Object o, TridentCollector tridentCollector) {
        return new State();
    }

    public void aggregate(State state, TridentTuple tridentTuple, TridentCollector tridentCollector) {
        state.count = tridentTuple.getLong(0) + state.count;
    }

    public void complete(State state, TridentCollector tridentCollector) {
        tridentCollector.emit(new Values(state.count));
    }

    static class State {
        long count = 0;
    }
}
