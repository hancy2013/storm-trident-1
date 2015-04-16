package com.skyfree.trident;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

import java.util.*;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/4/16 下午5:27
 */
public class FakeTweetSpout implements IBatchSpout {
    private static final long serialVersionUID = 1L;
    private int batchSize;

    private HashMap<Long, List<List<Object>>> batchesMap = new HashMap<Long, List<List<Object>>>();

    private static final Map<Integer, String> TWEET_MAP = new HashMap<Integer, String>();

    private static final Map<Integer, String> COUNTRY_MAP = new HashMap<Integer, String>();

    static {
        TWEET_MAP.put(0, " Adidas #FIFA World Cup Chant Challenge ");
        TWEET_MAP.put(1, "#FIFA worldcup");
        TWEET_MAP.put(2, "#FIFA worldcup");
        TWEET_MAP.put(3, " The Great Gatsby is such a good #movie ");
        TWEET_MAP.put(4, "#Movie top 10");

        COUNTRY_MAP.put(0, "United State");
        COUNTRY_MAP.put(1, "Japan");
        COUNTRY_MAP.put(2, "India");
        COUNTRY_MAP.put(3, "China");
        COUNTRY_MAP.put(4, "Brazil");
    }

    private List<Object> recordGenerator() {
        final Random rand = new Random();
        int randomNumber = rand.nextInt(5);
        int randomNumber2 = rand.nextInt(5);
        return new Values(TWEET_MAP.get(randomNumber), COUNTRY_MAP.get(randomNumber2));
    }

    public FakeTweetSpout(int batchSize) {
        this.batchSize = batchSize;
    }

    public void open(Map map, TopologyContext topologyContext) {
        // 用于初始化变量，打开外部数据源连接等
    }

    public void emitBatch(long batchId, TridentCollector tridentCollector) {
        List<List<Object>> batches = this.batchesMap.get(batchId);
        if (batches == null) {
            batches = new ArrayList<List<Object>>();
            for (int i = 0; i < this.batchSize; i++) {
                batches.add(this.recordGenerator());
            }
            this.batchesMap.put(batchId, batches);
        }

        for (List<Object> list : batches) {
            tridentCollector.emit(list);
        }
    }

    public void ack(long batchId) {
        this.batchesMap.remove(batchId);
    }

    public void close() {
        //销毁在open方法中打开的外部连接
    }

    public Map getComponentConfiguration() {
        // 在这里还可以设定spout的配置，可以定义spout的并行参数
        return null;
    }

    public Fields getOutputFields() {
        // 确定了emit的字段
        return new Fields("text", "country");
    }
}
