package com.skyfree.trident.filter;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/8 17:46
 */
public class CheckEvenSumFilter extends BaseFilter {
    public boolean isKeep(TridentTuple tridentTuple) {
        int number1 = tridentTuple.getInteger(0);
        int number2 = tridentTuple.getInteger(1);

        return (number1 + number2) % 2 == 0;
    }
}
