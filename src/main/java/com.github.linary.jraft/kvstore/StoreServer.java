/*
 * Copyright (C) 2020 Baidu, Inc. All Rights Reserved.
 */
package com.github.linary.jraft.kvstore;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;

public class StoreServer {

    private Node node;
    private StoreStateMachine fsm;

    public StoreServer(final String dataPath, final String groupId, final PeerId serverId,
                       final NodeOptions nodeOptions) throws IOException {
        // 初始化路径
        FileUtils.forceMkdir(new File(dataPath));

        // 这里让 raft RPC 和业务 RPC 使用同一个 RPC server, 通常也可以分开
        final RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.getEndpoint());
        StoreService storeService = new StoreServiceImpl(this);
        // 注册业务处理器，真正的业务逻辑实现的地方
        rpcServer.registerProcessor(new GetRequestProcessor(storeService));
        rpcServer.registerProcessor(new PutRequestProcessor(storeService));
        // 初始化状态机
        this.fsm = new StoreStateMachine();
        // 设置状态机到启动参数
        nodeOptions.setFsm(this.fsm);
        // 设置存储路径
        // 日志, 必须
        nodeOptions.setLogUri(dataPath + File.separator + "log");
        // 元信息, 必须
        nodeOptions.setRaftMetaUri(dataPath + File.separator + "raft_meta");
        // snapshot, 可选, 一般都推荐
        nodeOptions.setSnapshotUri(dataPath + File.separator + "snapshot");
        // 初始化 raft group 服务框架
        RaftGroupService raftGroupService = new RaftGroupService(groupId, serverId,
                                                                 nodeOptions, rpcServer);
        // 启动
        this.node = raftGroupService.start();
    }

    public Node getNode() {
        return node;
    }

    public StoreStateMachine getFsm() {
        return fsm;
    }

    public ValueResponse redirect() {
        final ValueResponse response = new ValueResponse();
        response.setSuccess(false);
        if (this.node != null) {
            final PeerId leader = this.node.getLeaderId();
            if (leader != null) {
                response.setRedirect(leader.toString());
            }
        }
        return response;
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 4) {
            System.out.println("Useage : StoreServer {dataPath} {groupId} {serverId} {initConf}");
            System.out.println("Example: StoreServer /tmp/server1 store 127.0.0.1:8081 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083");
            System.exit(1);
        }
        final String dataPath = args[0];
        final String groupId = args[1];
        final String serverIdStr = args[2];
        final String initConfStr = args[3];

        final NodeOptions nodeOptions = new NodeOptions();
        // 为了测试,调整 snapshot 间隔等参数
        // 设置选举超时时间为 1 秒
        nodeOptions.setElectionTimeoutMs(1000);
        // 关闭 CLI 服务
        nodeOptions.setDisableCli(false);
        // 每隔30秒做一次 snapshot
        nodeOptions.setSnapshotIntervalSecs(30);
        // 解析参数
        final PeerId serverId = new PeerId();
        if (!serverId.parse(serverIdStr)) {
            throw new IllegalArgumentException("Fail to parse serverId:" + serverIdStr);
        }
        final Configuration initConf = new Configuration();
        if (!initConf.parse(initConfStr)) {
            throw new IllegalArgumentException("Fail to parse initConf:" + initConfStr);
        }
        // 设置初始集群配置
        nodeOptions.setInitialConf(initConf);

        // 启动
        final StoreServer storeServer = new StoreServer(dataPath, groupId, serverId, nodeOptions);
        System.out.println("Started counter server at port:" +
                           storeServer.getNode().getNodeId().getPeerId().getPort());
    }
}
