package com.skyfree.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/4/16 下午5:38
 */
public class TridentSimpleTopology {
    public static void main(String[] args) throws Exception {
        simpleTopology(args);
    }

    /**
     * 提交一个非事务性的trident-topology
     *
     * @param args 命令行参数
     */
    private static void simpleTopology(String[] args) {
        Config config = new Config();

        config.setMaxSpoutPending(20);

        if (args[0].equals("local")) {
            // 本地测试提交
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("Count", config, buildSimpleTopology());
        } else {
            // 提交到服务器
            config.setNumWorkers(3);
            try {
                StormSubmitter.submitTopology("trident_simple_topology", config, buildSimpleTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 构造一个非事务性的trident-topology
     *
     * @return 返回topology
     */
    private static StormTopology buildSimpleTopology() {
        // 10是每批次的元组数量
        FakeTweetSpout spout = new FakeTweetSpout(10);

        // 创建了一个TridentTopology对象,之后基于这个对象创建计算拓扑
        TridentTopology topology = new TridentTopology();

        /**
         * TridentTopology实例的newStream方法定义一个新的数据流,参数是一个spout
         * .shuffle(): 传入的元组随机进入后续的处理节点
         * .each(new Fields("text", "country"), new TridentUtility.TweetFilter("#FIFA")): 处理含有text和country字段的每个元组,进行过滤操作,含有#FIFA的留下,不含有的,直接移除
         * .groupBy(new Fields("country")): 按照country进行分组
         * .aggregate(new Fields("country"), new Count(), new Fields("count")): 处理含有country字段的元组进行计数, 新输出的元组只含有count字段
         * .each(new Fields("count"), new TridentUtility.Print()): 处理含有count字段的元组,进行Print过滤操作, 这里只会打印出一个数字,
         *      问题: 如果我希望知道group的名字呢?我只是简单的添加了.each(new Fields("country", "count"), new TridentUtility.Print())
         * .parallelismHint(2); 指定并行度,怎么个计算方法???
         */
        topology.newStream("faketweetspout", spout)
                .shuffle()
                .each(new Fields("text", "country"), new TridentUtility.TweetFilter("#FIFA"))
                .groupBy(new Fields("country"))
                .aggregate(new Fields("country"), new Count(), new Fields("count"))
                .each(new Fields("country", "count"), new TridentUtility.Print())
                .parallelismHint(2);

        return topology.build();
    }
}
