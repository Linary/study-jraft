/*
 * Copyright (C) 2020 Baidu, Inc. All Rights Reserved.
 */
package com.github.linary.jraft.kvstore;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;

public class GetRequestProcessor implements RpcProcessor<GetRequest> {

    private final StoreService service;

    public GetRequestProcessor(StoreService service) {
        this.service = service;
    }

    @Override
    public void handleRequest(RpcContext context, GetRequest request) {
        final StoreClosure closure = new StoreClosure() {
            @Override
            public void run(Status status) {
                context.sendResponse(getValueResponse());
            }
        };
        this.service.get(request.getKey(), request.isReadOnlySafe(), closure);
    }

    @Override
    public String interest() {
        return GetRequest.class.getName();
    }
}
