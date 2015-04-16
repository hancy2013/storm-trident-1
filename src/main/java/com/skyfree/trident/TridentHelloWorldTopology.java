package com.skyfree.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.DRPCClient;
import org.apache.thrift7.TException;
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
public class TridentHelloWorldTopology {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException, TException, DRPCExecutionException {
        Config config = new Config();
        config.setMaxSpoutPending(20);
        LocalDRPC drpc = new LocalDRPC();

        if (args.length == 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("Count", config, buildTopology(drpc));
            Thread.sleep(2000);
            for (int i = 0; i < 100; i++) {
                System.out.println(drpc.execute("Count", "Japan,India,Europe"));
                Thread.sleep(1000);
            }
        } else {
            config.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], config, buildTopology(drpc));
            Thread.sleep(2000);
            DRPCClient client = new DRPCClient("DRPC-SERVER-IP", 1234);
            System.out.println(client.execute("Count", "Japan,India,Europe,China"));
        }
    }

    private static StormTopology buildTopology() {
        FakeTweetSpout spout = new FakeTweetSpout(10);
        TridentTopology topology = new TridentTopology();
        topology.newStream("faketweetspout", spout)
                .shuffle()
                .each(new Fields("text", "country"), new TridentUtility.TweetFilter())
                .groupBy(new Fields("country"))
                .aggregate(new Fields("country"), new Count(), new Fields("count"))
                .each(new Fields("count"), new TridentUtility.Print())
                .parallelismHint(2);
        return topology.build();
    }

    private static StormTopology buildTopology(LocalDRPC drpc) throws InterruptedException {
        FakeTweetSpout spout = new FakeTweetSpout(10);
        TridentTopology topology = new TridentTopology();

        TridentState countryCount = topology.newStream("spout", spout)
                .shuffle()
                .each(new Fields("text", "country"), new TridentUtility.TweetFilter())
                .groupBy(new Fields("country"))
                .persistentAggregate(new MemoryMapState.Factory(), new Fields("country"), new Count(), new Fields("count"))
                .parallelismHint(2);


        Thread.sleep(2000);

        topology.newDRPCStream("Count", drpc)
                .each(new Fields("args"), new TridentUtility.Split(), new Fields("country"))
                .stateQuery(countryCount, new Fields("country"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull());
        
        return topology.build();
    }
}
