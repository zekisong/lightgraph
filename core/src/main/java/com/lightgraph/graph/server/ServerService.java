package com.lightgraph.graph.server;

import com.lightgraph.graph.cluster.Replication;
import com.lightgraph.graph.meta.EdgeMeta;
import com.lightgraph.graph.meta.VertexMeta;
import com.lightgraph.graph.meta.cluster.GraphMeta;
import com.lightgraph.graph.modules.rpc.MetaRpcService;
import com.lightgraph.graph.modules.rpc.StorageRpcService;
import com.lightgraph.graph.modules.storage.KeyValue;

import java.util.List;
import java.util.Set;

public interface ServerService extends StorageRpcService {
    MetaRpcService getMaster();

    MetaRpcService getMasterLeader();

    Replication getPartitionLeader(String graph, KeyValue keyValue);

    Set<Replication> getReplications(String graph, KeyValue keyValue);

    GraphMeta getGraphMeta(String graph);

    VertexMeta getVertexMeta(String graph, String name);

    EdgeMeta getEdgeMeta(String graph, String name);

    List<GraphMeta> listGraphMeta();
}
