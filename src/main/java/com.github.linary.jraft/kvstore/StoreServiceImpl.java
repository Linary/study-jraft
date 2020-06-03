/*
 * Copyright (C) 2020 Baidu, Inc. All Rights Reserved.
 */
package com.github.linary.jraft.kvstore;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;
import com.alipay.sofa.jraft.util.Utils;

public class StoreServiceImpl implements StoreService {

    private static final Logger LOG = LoggerFactory.getLogger(StoreServiceImpl.class);

    private final StoreServer server;
    private final Executor readIndexExecutor;

    public StoreServiceImpl(StoreServer server) {
        this.server = server;
        this.readIndexExecutor = createReadIndexExecutor();
    }

    private Executor createReadIndexExecutor() {
        return createReadIndexExecutor(Math.max(Utils.cpus() << 2, 16));
    }

    public static ExecutorService createReadIndexExecutor(final int coreThreads) {
        final int maxThreads = coreThreads << 2;
        final RejectedExecutionHandler handler = new ThreadPoolExecutor.AbortPolicy();
        return newPool(coreThreads, maxThreads, "kvstore-read-index-callback", handler);
    }

    @Override
    public void get(int key, boolean readOnlySafe, StoreClosure closure) {
        if (!readOnlySafe) {
            closure.success(getValue(key));
            closure.run(Status.OK());
            return;
        }

        ReadIndexClosure done = new ReadIndexClosure() {
            @Override
            public void run(Status status, long index, byte[] reqCtx) {
                if (status.isOk()) {
                    closure.success(getValue(key));
                    closure.run(Status.OK());
                    return;
                }
                StoreServiceImpl.this.readIndexExecutor.execute(() -> {
                    if (isLeader()) {
                        LOG.debug("Fail to get value with 'ReadIndex': "
                                        + "{}, try to applying to the"
                                        + " state machine.", status);
                        applyOperation(StoreOperation.createGet(key), closure);
                    } else {
                        handlerNotLeaderError(closure);
                    }
                });
            }
        };
        this.server.getNode().readIndex(BytesUtil.EMPTY_BYTES, done);
    }

    @Override
    public void put(int key, Object value, StoreClosure closure) {
        applyOperation(StoreOperation.createPut(key, value), closure);
    }

    private void applyOperation(final StoreOperation op, final StoreClosure closure) {
        if (!isLeader()) {
            handlerNotLeaderError(closure);
            return;
        }

        try {
            closure.setStoreOperation(op);
            final Task task = new Task();
            task.setData(ByteBuffer.wrap(SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(op)));
            task.setDone(closure);
            // 提交task到当前node上
            this.server.getNode().apply(task);
        } catch (CodecException e) {
            String errorMsg = "Fail to encode CounterOperation";
            LOG.error(errorMsg, e);
            closure.failure(errorMsg, StringUtils.EMPTY);
            closure.run(new Status(RaftError.EINTERNAL, errorMsg));
        }
    }

    private void handlerNotLeaderError(final StoreClosure closure) {
        closure.failure("Not leader.", getRedirect());
        closure.run(new Status(RaftError.EPERM, "Not leader"));
    }

    private boolean isLeader() {
        return this.server.getFsm().isLeader();
    }

    private Object getValue(int key) {
        return this.server.getFsm().getValue(key);
    }

    private String getRedirect() {
        return this.server.redirect().getRedirect();
    }

    private static ExecutorService newPool(final int coreThreads,
                                           final int maxThreads,
                                           final String name,
                                           final RejectedExecutionHandler handler) {
        final BlockingQueue<Runnable> defaultWorkQueue =
                new SynchronousQueue<>();
        return newPool(coreThreads, maxThreads, defaultWorkQueue, name,
                       handler);
    }

    private static ExecutorService newPool(final int coreThreads,
                                           final int maxThreads,
                                           final BlockingQueue<Runnable> workQueue,
                                           final String name,
                                           final RejectedExecutionHandler handler) {
        return ThreadPoolUtil.newBuilder() //
                             .poolName(name) //
                             .enableMetric(true) //
                             .coreThreads(coreThreads) //
                             .maximumThreads(maxThreads) //
                             .keepAliveSeconds(60L) //
                             .workQueue(workQueue) //
                             .threadFactory(new NamedThreadFactory(name, true))
                             .rejectedHandler(handler) //
                             .build();
    }
}
