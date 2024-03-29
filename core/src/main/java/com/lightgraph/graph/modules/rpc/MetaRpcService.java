package com.lightgraph.graph.modules.rpc;

import com.lightgraph.graph.cluster.Replication;
import com.lightgraph.graph.cluster.node.Node;
import com.lightgraph.graph.graph.EdgeMetaInfo;
import com.lightgraph.graph.graph.VertexMetaInfo;
import com.lightgraph.graph.meta.EdgeMeta;
import com.lightgraph.graph.meta.LabelMeta;
import com.lightgraph.graph.meta.LabelType;
import com.lightgraph.graph.meta.MetaType;
import com.lightgraph.graph.meta.VertexMeta;
import com.lightgraph.graph.meta.cluster.GraphMeta;
import com.lightgraph.graph.settings.GraphSetting;

import java.util.List;

public interface MetaRpcService extends SyncRpcService {

    Node getLeader();

    boolean joinToCluster(Node node);

    List<byte[]> updateContext(long version);

    long getNodeVersion();

    boolean createGraph(GraphSetting setting);

    boolean addReplications(List<Replication> replications);

    boolean updateReplication(Replication replication);

    LabelMeta getLabelMetaById(Long id, LabelType type);

    GraphMeta getGraphMeta(String graph);

    List<GraphMeta> listGraphMeta();

    List<LabelMeta> listLabelMeta(String graph, MetaType metaType);

    boolean addVertexMeta(VertexMetaInfo vertexMeta);

    boolean addEdgeMeta(EdgeMetaInfo edgeMetaInfo);

    VertexMeta getVertexMeta(String graph, String name);

    EdgeMeta getEdgeMeta(String graph, String name);
}