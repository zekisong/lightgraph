package com.lightgraph.graph.modules.rpc;

import com.lightgraph.graph.modules.Module;
import com.lightgraph.graph.modules.NetworkModule;
import com.lightgraph.graph.cluster.node.Node;

public interface RpcServiceModule extends Module {
    void bindNetwork(NetworkModule networkModule);

    void bindRpcService(MetaRpcService metaRpcService, StorageRpcService dataRpcService);

    <T> T getRpcServiceHandler(Node node);
}
