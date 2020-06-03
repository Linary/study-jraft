/*
 * Copyright (C) 2020 Baidu, Inc. All Rights Reserved.
 */
package com.github.linary.jraft.kvstore;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;

public class PutRequestProcessor implements RpcProcessor<PutRequest> {

    private final StoreService service;

    public PutRequestProcessor(StoreService service) {
        this.service = service;
    }

    @Override
    public void handleRequest(RpcContext context, PutRequest request) {
        final StoreClosure closure = new StoreClosure() {
            @Override
            public void run(Status status) {
                context.sendResponse(getValueResponse());
            }
        };
        this.service.put(request.getKey(), request.getValue(), closure);
    }

    @Override
    public String interest() {
        return PutRequest.class.getName();
    }
}
