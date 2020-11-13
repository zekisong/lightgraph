package rpc;

import com.google.protobuf.ByteString;
import com.lightgraph.graph.cluster.Replication;
import com.lightgraph.graph.graph.EdgeMetaInfo;
import com.lightgraph.graph.graph.VertexMetaInfo;
import com.lightgraph.graph.meta.EdgeMeta;
import com.lightgraph.graph.meta.VertexMeta;
import com.lightgraph.graph.meta.cluster.GraphMeta;
import com.lightgraph.graph.modules.rpc.StorageRpcService;
import com.lightgraph.graph.modules.rpc.MetaRpcService;
import com.lightgraph.graph.modules.rpc.ServiceType;
import com.lightgraph.graph.cluster.node.Node;
import com.lightgraph.graph.modules.storage.KeyValue;
import com.lightgraph.graph.settings.GraphSetting;
import com.lightgraph.raft.proto.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

public class RpcClient implements MetaRpcService, StorageRpcService {

    private static final Log LOG = LogFactory.getLog(RpcClient.class);
    private RpcServiceGrpc.RpcServiceBlockingStub stub;

    public RpcClient(RpcServiceGrpc.RpcServiceBlockingStub stub) {
        this.stub = stub;
    }

    @Override
    public Node getLeader() {
        GetMasterLeaderRequest request = GetMasterLeaderRequest.newBuilder().build();
        Node leader;
        try {
            GetMasterLeaderResponse response = stub.getMasterLeader(request);
            if (!response.getLeader().isEmpty()) {
                leader = new Node(response.getLeader().toByteArray());
                return leader;
            } else {
                return null;
            }
        } catch (Exception e) {
            LOG.error("get master leader failed!", e);
            return null;
        }
    }

    @Override
    public boolean joinToCluster(Node node) {
        JoinToClusterRequest.Builder builder = JoinToClusterRequest.newBuilder();
        builder.setNode(ByteString.copyFrom(node.getBytes()));
        try {
            JoinToClusterResponse response = stub.joinToCluster(builder.build());
            return response.getSuccess();
        } catch (Exception e) {
            LOG.error("join to cluster failed!", e);
            throw e;
        }
    }

    @Override
    public List<byte[]> updateContext(long version) {
        UpdateContextRequest request = UpdateContextRequest.newBuilder()
                .setVersion(version)
                .build();
        try {
            UpdateContextResponse response = stub.updateContext(request);
            List<byte[]> deltas = new ArrayList<>();
            response.getDeltasList().forEach(d -> deltas.add(d.toByteArray()));
            return deltas;
        } catch (Exception e) {
            LOG.error("update context failed!", e);
            return null;
        }
    }

    @Override
    public long getNodeVersion() {
        GetNodeVersionRequest request = GetNodeVersionRequest.newBuilder().build();
        try {
            GetNodeVersionResponse response = stub.getNodeVersion(request);
            return response.getVersion();
        } catch (Exception e) {
            LOG.error("get node version failed!", e);
            return -1;
        }
    }

    @Override
    public boolean createGraph(GraphSetting setting) {
        CreateGraphRequest request = CreateGraphRequest.newBuilder().setSetting(ByteString.copyFrom(setting.getBytes())).build();
        try {
            CreateGraphResponse response = stub.createGraph(request);
            return response.getSuccess();
        } catch (Exception e) {
            LOG.error("create graph failed!", e);
            throw e;
        }
    }

    @Override
    public boolean addReplications(List<Replication> replications) {
        AddReplicationsRequest.Builder builder = AddReplicationsRequest.newBuilder();
        replications.forEach(replication -> builder.addReplications(ByteString.copyFrom(replication.getBytes())));
        try {
            AddReplicationsResponse response = stub.addReplications(builder.build());
            return response.getSuccess();
        } catch (Exception e) {
            LOG.error("add replications failed!", e);
            throw e;
        }
    }

    @Override
    public boolean updateReplication(Replication replication) {
        UpdateReplicationRequest request = UpdateReplicationRequest.newBuilder().setReplication(ByteString.copyFrom(replication.getBytes())).build();
        try {
            UpdateReplicationResponse response = stub.updateReplication(request);
            return response.getSuccess();
        } catch (Exception e) {
            LOG.error("update replication failed!", e);
            throw e;
        }
    }

    @Override
    public boolean put(String graph, KeyValue keyValue) {
        PutRequest request = PutRequest.newBuilder().setGraph(graph).setKeyValue(ByteString.copyFrom(keyValue.getBytes())).build();
        try {
            PutResponse response = stub.putData(request);
            return response.getSuccess();
        } catch (Exception e) {
            LOG.error("put failed!", e);
            throw e;
        }
    }

    @Override
    public List<KeyValue> scan(String graph, KeyValue keyValue) {
        PrefixScanRequest request = PrefixScanRequest.newBuilder().setGraph(graph).setPrefix(ByteString.copyFrom(keyValue.getBytes())).build();
        try {
            PrefixScanResponse response = stub.prefixScan(request);
            List<KeyValue> keyValues = new ArrayList<>();
            response.getResultList().forEach(kv -> {
                keyValues.add(new KeyValue(kv.toByteArray()));
            });
            return keyValues;
        } catch (Exception e) {
            LOG.error("get failed!", e);
            throw e;
        }
    }

    @Override
    public GraphMeta getGraphMeta(String graph) {
        GetGraphMetaRequest request = GetGraphMetaRequest.newBuilder().setGraph(graph).build();
        try {
            GetGraphMetaResponse response = stub.getGraphMeta(request);
            return new GraphMeta(response.getResult().toByteArray());
        } catch (Exception e) {
            LOG.error("put failed!", e);
            throw e;
        }
    }

    @Override
    public List<GraphMeta> listGraphMeta() {
        ListGraphMetaRequest request = ListGraphMetaRequest.newBuilder().build();
        try {
            ListGraphMetaResponse response = stub.listGraphMeta(request);
            List<GraphMeta> metas = new ArrayList<>();
            response.getResultList().forEach(r -> {
                metas.add(new GraphMeta(r.toByteArray()));
            });
            return metas;
        } catch (Exception e) {
            LOG.error("put failed!", e);
            throw e;
        }
    }

    @Override
    public boolean addVertexMeta(VertexMetaInfo vertexMetaInfo) {
        AddVertexMetaRequest.Builder builder = AddVertexMetaRequest.newBuilder().setVertexMeta(ByteString.copyFrom(vertexMetaInfo.getBytes()));
        try {
            AddVertexMetaResponse response = stub.addVertexMeta(builder.build());
            return true;
        } catch (Exception e) {
            LOG.error("add vertex meta failed!", e);
            throw e;
        }
    }

    @Override
    public boolean addEdgeMeta(EdgeMetaInfo edgeMetaInfo) {
        AddEdgeMetaRequest.Builder builder = AddEdgeMetaRequest.newBuilder().setEdgeMeta(ByteString.copyFrom(edgeMetaInfo.getBytes()));
        try {
            AddEdgeMetaResponse response = stub.addEdgeMeta(builder.build());
            return true;
        } catch (Exception e) {
            LOG.error("add edge meta failed!", e);
            throw e;
        }
    }

    @Override
    public VertexMeta getVertexMeta(String graph, String name) {
        GetVertexMetaRequest.Builder builder = GetVertexMetaRequest.newBuilder().setGraph(graph).setName(name);
        try {
            GetVertexMetaResponse response = stub.getVertexMeta(builder.build());
            return new VertexMeta(response.getResult().toByteArray());
        } catch (Exception e) {
            LOG.error("add vertex meta failed!", e);
            throw e;
        }
    }

    @Override
    public EdgeMeta getEdgeMeta(String graph, String name) {
        GetEdgeMetaRequest.Builder builder = GetEdgeMetaRequest.newBuilder().setGraph(graph).setName(name);
        try {
            GetEdgeMetaResponse response = stub.getEdgeMeta(builder.build());
            return new EdgeMeta(response.getResult().toByteArray());
        } catch (Exception e) {
            LOG.error("add vertex meta failed!", e);
            throw e;
        }
    }

    @Override
    public KeyValue get(String graph, KeyValue keyValue) {
        GetRequest request = GetRequest.newBuilder().setGraph(graph).setKey(ByteString.copyFrom(keyValue.getBytes())).build();
        try {
            GetResponse response = stub.getData(request);
            return new KeyValue(response.getKeyValue().toByteArray());
        } catch (Exception e) {
            LOG.error("get failed!", e);
            throw e;
        }
    }

    @Override
    public ServiceType getServiceType() {
        return null;
    }
}
