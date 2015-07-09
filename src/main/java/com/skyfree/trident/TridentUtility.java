package com.skyfree.trident;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.tuple.TridentTuple;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/4/16 下午5:19
 */
public class TridentUtility {
    /**
     * trident的function
     */
    public static class Split extends BaseFunction {
        private static final long serialVersionUID = 1L;

        public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
            String countries = tridentTuple.getString(0);
            for (String word : countries.split(",")) {
                tridentCollector.emit(new Values(word));
            }
        }
    }

    /**
     * trident的filter
     */
    public static class TweetFilter extends BaseFilter {
        private static final long serialVersionUID = 2L;
        private String prefix;

        public TweetFilter(String prefix) {
            this.prefix = prefix;
        }

        public boolean isKeep(TridentTuple tridentTuple) {
            if (tridentTuple.getString(0).contains(prefix)) {
                return true;
            } else {
                return false;
            }
        }
    }

    /**
     * trident的filter,只执行一个附加操作
     */
    public static class Print extends BaseFilter {
        private static final long serialVersionUID = 3L;

        public boolean isKeep(TridentTuple tridentTuple) {
            System.out.println(tridentTuple);
            return true;
        }
    }
}
