package com.skyfree.trident;

import backtype.storm.utils.DRPCClient;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/9 12:55
 */

/**
 * 这个客户端可以运行在任何地方,直接访问storm,返回当时的结果
 */
public class RcpClient {
    public static void main(String[] args) throws Exception {
        DRPCClient client = new DRPCClient("storm1", 3772);
        for (int i = 0; i < 100; i++) {
            Thread.sleep(2000);
            System.out.println(client.execute("Count", "Japan,India,Europe"));
        }
    }
}
