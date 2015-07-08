package com.skyfree.trident.function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/8 17:28
 */
public class SumFunction extends BaseFunction {
    private static final long serialVersionUID = 5L;

    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        int number1 = tridentTuple.getInteger(0);
        int number2 = tridentTuple.getInteger(1);

        tridentCollector.emit(new Values(number1 + number2));
    }
}
