/*
 * Copyright (C) 2020 Baidu, Inc. All Rights Reserved.
 */
package com.github.linary.jraft.kvstore;

import com.alipay.sofa.jraft.Closure;

public abstract class StoreClosure implements Closure {

    private ValueResponse valueResponse;
    private StoreOperation storeOperation;

    public ValueResponse getValueResponse() {
        return valueResponse;
    }

    public void setValueResponse(ValueResponse valueResponse) {
        this.valueResponse = valueResponse;
    }

    public StoreOperation getStoreOperation() {
        return storeOperation;
    }

    public void setStoreOperation(StoreOperation storeOperation) {
        this.storeOperation = storeOperation;
    }

    protected void success(final Object value) {
        final ValueResponse response = new ValueResponse();
        response.setValue(value);
        response.setSuccess(true);
        setValueResponse(response);
    }

    protected void failure(final String errorMsg, final String redirect) {
        final ValueResponse response = new ValueResponse();
        response.setSuccess(false);
        response.setErrorMsg(errorMsg);
        response.setRedirect(redirect);
        setValueResponse(response);
    }
}
