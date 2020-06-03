/*
 * Copyright (C) 2020 Baidu, Inc. All Rights Reserved.
 */
package com.github.linary.jraft.kvstore;

import static com.github.linary.jraft.kvstore.StoreOperation.GET;
import static com.github.linary.jraft.kvstore.StoreOperation.PUT;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.Utils;

public class StoreStateMachine extends StateMachineAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(StoreStateMachine.class);

    /**
     * Counter value
     */
    private final Map<Integer, Object> store = new ConcurrentHashMap<>();
    /**
     * Leader term，为什么需要这个
     */
    private final AtomicLong leaderTerm = new AtomicLong(-1);

    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }

    public Object getValue(int key) {
        return this.store.get(key);
    }

    @Override
    public void onApply(Iterator iter) {
        while (iter.hasNext()) {
            StoreOperation operation = null;

            StoreClosure closure = null;
            // 不为done表示是leader
            if (iter.done() != null) {
                // This task is applied by this node, get value from closure to avoid additional parsing.
                closure = (StoreClosure) iter.done();
                operation = closure.getStoreOperation();
            } else {
                // Have to parse FetchAddRequest from this user log.
                final ByteBuffer data = iter.getData();
                try {
                    operation = SerializerManager.getSerializer(SerializerManager.Hessian2)
                                                 .deserialize(data.array(), StoreOperation.class.getName());
                } catch (final CodecException e) {
                    LOG.error("Fail to decode Request", e);
                }
            }
            Object value = null;
            if (operation != null) {
                switch (operation.getOp()) {
                    case GET:
                        value = this.store.get(operation.getKey());
                        LOG.info("Get value={} at logIndex={}", value, iter.getIndex());
                        break;
                    case PUT:
                        this.store.put(operation.getKey(), operation.getValue());
                        value = this.store.get(operation.getKey());
                        LOG.info("Put key={} value={} at logIndex={}", operation.getKey(), operation.getValue(), iter.getIndex());
                        break;
                }

                if (closure != null) {
                    closure.success(value);
                    closure.run(Status.OK());
                }
            }
            iter.next();
        }
    }

    @Override
    public void onSnapshotSave(SnapshotWriter writer, Closure done) {
        Utils.runInThread(() -> {
            final StoreSnapshotFile snapshot = new StoreSnapshotFile(writer.getPath() + File.separator + "data");
            if (snapshot.save(this.store)) {
                if (writer.addFile("data")) {
                    done.run(Status.OK());
                } else {
                    done.run(new Status(RaftError.EIO, "Fail to add file to writer"));
                }
            } else {
                done.run(new Status(RaftError.EIO, "Fail to save counter snapshot %s", snapshot.getPath()));
            }
        });
    }

    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        if (isLeader()) {
            LOG.warn("Leader is not supposed to load snapshot");
            return false;
        }
        if (reader.getFileMeta("data") == null) {
            LOG.error("Fail to find data file in {}", reader.getPath());
            return false;
        }
        final StoreSnapshotFile snapshot = new StoreSnapshotFile(reader.getPath() + File.separator + "data");
        try {
            this.store.putAll(snapshot.load());
            return true;
        } catch (final IOException e) {
            LOG.error("Fail to load snapshot from {}", snapshot.getPath());
            return false;
        }
    }

    @Override
    public void onLeaderStart(final long term) {
        this.leaderTerm.set(term);
        super.onLeaderStart(term);

    }

    @Override
    public void onLeaderStop(final Status status) {
        this.leaderTerm.set(-1);
        super.onLeaderStop(status);
    }
}
