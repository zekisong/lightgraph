package com.lightgraph.graph.server;

import com.lightgraph.graph.cluster.Replication;
import com.lightgraph.graph.data.Edge;
import com.lightgraph.graph.data.Vertex;
import com.lightgraph.graph.meta.EdgeMeta;
import com.lightgraph.graph.meta.LabelMeta;
import com.lightgraph.graph.meta.MetaType;
import com.lightgraph.graph.meta.VertexMeta;
import com.lightgraph.graph.meta.cluster.GraphMeta;
import com.lightgraph.graph.modules.rpc.MetaRpcService;
import com.lightgraph.graph.modules.storage.Key;
import com.lightgraph.graph.modules.storage.KeyValue;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface ServerService {

    MetaRpcService getMaster();

    MetaRpcService getMasterLeader();

    Map<Replication, List<KeyValue>> getPartitionLeader(String graph, List<KeyValue> keyValues);

    Replication getPartitionLeader(String graph, Key key);

    Set<Replication> getReplications(String graph, Key key);

    GraphMeta getGraphMeta(String graph);

    VertexMeta getVertexMeta(String graph, String name);

    EdgeMeta getEdgeMeta(String graph, String name);

    List<GraphMeta> listGraphMeta();

    List<LabelMeta> listLabelMeta(String graph, MetaType metaType);

    boolean addVertex(String graph, String label, Long id, Map<String, Object> properties);

    boolean addEdge(String graph, String label, Long subject, Long object, Map<String, Object> properties);

    Vertex getVertex(String graph, String label, Long id);

    Edge getEdge(String graph, String label, Long subject, Long object);

}
