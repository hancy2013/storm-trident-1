package com.skyfree.trident.partition;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/8 18:11
 */
public class CountryRepartition implements CustomStreamGrouping {
    private static final long serialVersionUID = 1L;
    private int tasks = 0;
    private static final Map<String, Integer> countries = ImmutableMap.of(
            "India", 0,
            "Japan", 1,
            "United State", 2,
            "China", 3,
            "Brazil", 4
    );

    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId, List<Integer> list) {
        tasks = list.size();
    }

    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        String country = (String) values.get(0);
        return ImmutableList.of(countries.get(country) % tasks);
    }
}
