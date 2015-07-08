package com.skyfree.trident.aggregator;

import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/8 18:39
 */
public class SumReducerAggregator implements ReducerAggregator<Long> {
    public Long init() {
        return 0L;
    }

    public Long reduce(Long currentValue, TridentTuple tridentTuple) {
        return currentValue + tridentTuple.getLong(0);
    }
}
