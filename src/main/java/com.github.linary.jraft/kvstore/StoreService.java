/*
 * Copyright (C) 2020 Baidu, Inc. All Rights Reserved.
 */
package com.github.linary.jraft.kvstore;

/**
 * 声明提供什么服务
 */
public interface StoreService {

    /**
     * 因为这里发送请求之后是异步执行，结果用回调拿到，所以方法没有返回值
     * 需要传一个是否一致性读的参数
     */
    void get(int key, final boolean readOnlySafe, final StoreClosure closure);

    void put(int key, final Object value, final StoreClosure closure);
}
