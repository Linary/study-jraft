/*
 * Copyright (C) 2020 Baidu, Inc. All Rights Reserved.
 */
package com.github.linary.jraft.kvstore;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RemotingException;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;

public class StoreClient {

    public static void main(String[] args) throws Exception {
        args = new String[]{"store", "127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083"};
//        if (args.length != 2) {
//            System.out.println("Useage : java com.alipay.sofa.jraft.example.counter.CounterClient {groupId} {conf}");
//            System.out.println("Example: java com.alipay.sofa.jraft.example.counter.CounterClient counter 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083");
//            System.exit(1);
//        }
        final String groupId = args[0];
        final String confStr = args[1];

        final Configuration conf = new Configuration();
        if (!conf.parse(confStr)) {
            throw new IllegalArgumentException("Fail to parse conf:" + confStr);
        }

        RouteTable.getInstance().updateConfiguration(groupId, conf);

        final CliClientServiceImpl cliClientService = new CliClientServiceImpl();
        cliClientService.init(new CliOptions());

        if (!RouteTable.getInstance().refreshLeader(cliClientService, groupId, 1000).isOk()) {
            throw new IllegalStateException("Refresh leader failed");
        }

        final PeerId leader = RouteTable.getInstance().selectLeader(groupId);
        System.out.println("Leader is " + leader);
        final int n = 1000;
        final CountDownLatch latch = new CountDownLatch(n);
        final long start = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            put(cliClientService, leader, i, "value-" + i, latch);
        }
        latch.await();
        System.out.println(n + " ops, cost : " + (System.currentTimeMillis() - start) + " ms.");
        System.exit(0);
    }

    private static void put(final CliClientServiceImpl cliClientService, final PeerId leader,
                            final int key, final Object value, CountDownLatch latch)
                            throws RemotingException, InterruptedException {
        final PutRequest request = new PutRequest();
        request.setKey(key);
        request.setValue(value);
        cliClientService.getRpcClient().invokeAsync(leader.getEndpoint(), request, new InvokeCallback() {
            @Override
            public void complete(Object result, Throwable err) {
                if (err == null) {
                    latch.countDown();
                    System.out.println("put result:" + result);
                } else {
                    err.printStackTrace();
                    latch.countDown();
                }
            }
            @Override
            public Executor executor() {
                return null;
            }
        }, 5000);
    }
}
