package com.skyfree.trident.aggregator;

import clojure.lang.Numbers;
import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/8 19:03
 */
public class SumCombinerAggregator implements CombinerAggregator<Number> {

    private static final long serialVersionUID = 1L;

    public Number init(TridentTuple tridentTuple) {
        return (Number) tridentTuple.getValue(0);
    }

    public Number combine(Number number, Number t1) {
        return Numbers.add(number, t1);
    }

    public Number zero() {
        return 0;
    }
}
