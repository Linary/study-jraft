/*
 * Copyright (C) 2020 Baidu, Inc. All Rights Reserved.
 */
package com.github.linary.jraft.kvstore;

import java.io.Serializable;

public class PutRequest implements Serializable {

    private static final long serialVersionUID = -5623664785560971849L;

    private int key;
    private Object value;

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
