/*
 * Copyright (C) 2020 Baidu, Inc. All Rights Reserved.
 */
package com.github.linary.jraft.kvstore;

import java.io.Serializable;

public class StoreOperation implements Serializable {

    private static final long serialVersionUID = -6597003954824547294L;

    public static final byte GET = 0x01;
    public static final byte PUT = 0x02;

    private byte op;
    private Integer key;
    private Object value;

    public static StoreOperation createGet(final int key) {
        return new StoreOperation(GET, key, null);
    }

    public static StoreOperation createPut(final int key, final Object value) {
        return new StoreOperation(PUT, key, value);
    }

    public StoreOperation(byte op, Integer key, Object value) {
        this.op = op;
        this.key = key;
        this.value = value;
    }

    public byte getOp() {
        return op;
    }

    public Integer getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }
}
