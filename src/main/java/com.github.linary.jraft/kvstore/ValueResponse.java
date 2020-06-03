/*
 * Copyright (C) 2020 Baidu, Inc. All Rights Reserved.
 */
package com.github.linary.jraft.kvstore;

import java.io.Serializable;

/**
 * 所有请求的响应都是用这个结构来包装
 */
public class ValueResponse implements Serializable {

    private static final long serialVersionUID = -4220017686727146773L;

    private boolean success;
    private Object value;
    /**
     * redirect peer id
     */
    private String redirect;
    private String errorMsg;

    public ValueResponse() {
        super();
    }

    public ValueResponse(boolean success, Object value,
                         String redirect, String errorMsg) {
        super();
        this.success = success;
        this.value = value;
        this.redirect = redirect;
        this.errorMsg = errorMsg;
    }

    public boolean isSuccess() {
        return this.success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public Object getValue() {
        return this.value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public String getRedirect() {
        return this.redirect;
    }

    public void setRedirect(String redirect) {
        this.redirect = redirect;
    }

    public String getErrorMsg() {
        return this.errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    @Override
    public String toString() {
        return "ValueResponse [value=" + this.value + ", success="
                + this.success + ", redirect=" + this.redirect
                + ", errorMsg=" + this.errorMsg + "]";
    }
}
