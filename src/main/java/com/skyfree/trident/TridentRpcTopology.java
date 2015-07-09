package com.skyfree.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.testing.MemoryMapState;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/4/16 下午5:38
 */
public class TridentRpcTopology {
    public static void main(String[] args) throws Exception {
        drpcTopology(args);
    }
    
    private static void drpcTopology(String[] args) throws Exception {
        Config config = new Config();

        config.setMaxSpoutPending(20);
        LocalDRPC drpc = new LocalDRPC();

        if (args[0].equals("local")) {
            // 本地测试提交
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("Count", config, buildRpcTopology(drpc));
            
            for (int i = 0; i < 100; i++) {
                System.out.println(drpc.execute("Count", "Japan,India,Europe"));
                Thread.sleep(1000);
            }
        } else {
            // 提交到服务器
            config.setNumWorkers(3);
            
            try {
                StormSubmitter.submitTopology("trident_rpc_topology", config, buildRpcTopology(null));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        System.out.println("done!!!!!");
    }

    private static StormTopology buildRpcTopology(LocalDRPC drpc)  {
        FakeTweetSpout spout = new FakeTweetSpout(10);
        TridentTopology topology = new TridentTopology();

        TridentState countryCount = topology.newStream("spout", spout)
                .shuffle()
                .each(new Fields("text", "country"), new TridentUtility.TweetFilter("#FIFA"))
                .groupBy(new Fields("country"))
                .persistentAggregate(new MemoryMapState.Factory(), new Fields("country"), new Count(), new Fields("count"))
                .parallelismHint(2);
        

        // 这里定义了Count的操作
        topology.newDRPCStream("Count", drpc)
                .each(new Fields("args"), new TridentUtility.Split(), new Fields("country"))
                .stateQuery(countryCount, new Fields("country"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull());

        return topology.build();
    }
}
