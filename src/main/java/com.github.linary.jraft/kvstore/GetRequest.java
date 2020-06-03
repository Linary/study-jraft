/*
 * Copyright (C) 2020 Baidu, Inc. All Rights Reserved.
 */
package com.github.linary.jraft.kvstore;

import java.io.Serializable;

public class GetRequest implements Serializable {

    private static final long serialVersionUID = 9218253805003988802L;

    private int key;
    private boolean readOnlySafe = true;

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public boolean isReadOnlySafe() {
        return readOnlySafe;
    }

    public void setReadOnlySafe(boolean readOnlySafe) {
        this.readOnlySafe = readOnlySafe;
    }
}
