package rpc;

import com.google.protobuf.ByteString;
import com.lightgraph.graph.cluster.Replication;
import com.lightgraph.graph.graph.EdgeMetaInfo;
import com.lightgraph.graph.graph.VertexMetaInfo;
import com.lightgraph.graph.meta.EdgeMeta;
import com.lightgraph.graph.meta.LabelMeta;
import com.lightgraph.graph.meta.LabelType;
import com.lightgraph.graph.meta.MetaType;
import com.lightgraph.graph.meta.VertexMeta;
import com.lightgraph.graph.meta.cluster.GraphMeta;
import com.lightgraph.graph.modules.rpc.StorageRpcService;
import com.lightgraph.graph.modules.rpc.MetaRpcService;
import com.lightgraph.graph.cluster.node.Node;
import com.lightgraph.graph.modules.storage.Batch;
import com.lightgraph.graph.modules.storage.Key;
import com.lightgraph.graph.modules.storage.KeyValue;
import com.lightgraph.graph.settings.GraphSetting;
import com.lightgraph.raft.proto.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RpcServer extends RpcServiceGrpc.RpcServiceImplBase {

    private MetaRpcService metaRpcService;
    private StorageRpcService dataRpcService;

    public RpcServer(MetaRpcService metaRpcService, StorageRpcService dataRpcService) {
        this.metaRpcService = metaRpcService;
        this.dataRpcService = dataRpcService;
    }

    @Override
    public void joinToCluster(com.lightgraph.raft.proto.JoinToClusterRequest request,
            io.grpc.stub.StreamObserver<com.lightgraph.raft.proto.JoinToClusterResponse> responseObserver) {
        JoinToClusterResponse.Builder response = JoinToClusterResponse.newBuilder();
        Node node = Node.getInstance(request.getNode().toByteArray(), Node.class);
        boolean ret = metaRpcService.joinToCluster(node);
        response.setSuccess(ret);
        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getMasterLeader(com.lightgraph.raft.proto.GetMasterLeaderRequest request,
            io.grpc.stub.StreamObserver<com.lightgraph.raft.proto.GetMasterLeaderResponse> responseObserver) {
        GetMasterLeaderResponse.Builder builder = GetMasterLeaderResponse.newBuilder();
        Node leader = metaRpcService.getLeader();
        if (leader != null) {
            builder.setLeader(ByteString.copyFrom(leader.getBytes()));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateContext(com.lightgraph.raft.proto.UpdateContextRequest request,
            io.grpc.stub.StreamObserver<com.lightgraph.raft.proto.UpdateContextResponse> responseObserver) {
        UpdateContextResponse.Builder builder = UpdateContextResponse.newBuilder();
        long version = request.getVersion();
        List<byte[]> deltas = metaRpcService.updateContext(version);
        deltas.forEach(d -> builder.addDeltas(ByteString.copyFrom(d)));
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void createGraph(com.lightgraph.raft.proto.CreateGraphRequest request,
            io.grpc.stub.StreamObserver<com.lightgraph.raft.proto.CreateGraphResponse> responseObserver) {
        CreateGraphResponse.Builder builder = CreateGraphResponse.newBuilder();
        boolean ret = metaRpcService
                .createGraph(GraphSetting.getInstance(request.getSetting().toByteArray(), GraphSetting.class));
        builder.setSuccess(ret);
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void putData(com.lightgraph.raft.proto.PutRequest request,
            io.grpc.stub.StreamObserver<com.lightgraph.raft.proto.PutResponse> responseObserver) {
        PutResponse.Builder builder = PutResponse.newBuilder();
        String graph = request.getGraph();
        KeyValue kv = KeyValue.getInstance(request.getKeyValue().toByteArray(), KeyValue.class);
        boolean ret = dataRpcService.put(graph, kv);
        builder.setSuccess(ret);
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void batchPut(com.lightgraph.raft.proto.BatchPutRequest request,
            io.grpc.stub.StreamObserver<com.lightgraph.raft.proto.BatchPutResponse> responseObserver) {
        BatchPutResponse.Builder builder = BatchPutResponse.newBuilder();
        String graph = request.getGraph();
        boolean ret = dataRpcService
                .batchPut(graph, Replication.getInstance(request.getReplication().toByteArray(), Replication.class),
                        Batch.getInstance(request.getBatch().toByteArray(), Batch.class));
        builder.setSuccess(ret);
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();

    }

    @Override
    public void getData(com.lightgraph.raft.proto.GetRequest request,
            io.grpc.stub.StreamObserver<com.lightgraph.raft.proto.GetResponse> responseObserver) {
        GetResponse.Builder builder = GetResponse.newBuilder();
        String graph = request.getGraph();
        Key key = Key.warp(request.getKey().toByteArray());
        byte[] result = dataRpcService.get(graph, key).getBytes();
        builder.setKeyValue(ByteString.copyFrom(result));
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void prefixScan(com.lightgraph.raft.proto.PrefixScanRequest request,
            io.grpc.stub.StreamObserver<com.lightgraph.raft.proto.PrefixScanResponse> responseObserver) {
        String graph = request.getGraph();
        Iterator<KeyValue> it = dataRpcService
                .scan(graph, Key.warp(request.getPrefix().toByteArray()));
        while (true) {
            List<ByteString> batch = new ArrayList<>();
            int batchSize = 1000;
            while (it.hasNext()) {
                batch.add(ByteString.copyFrom(it.next().getBytes()));
                batchSize = batchSize - 1;
                if (batchSize == 0) {
                    break;
                }
            }
            if (batch.size() > 0) {
                PrefixScanResponse.Builder builder = PrefixScanResponse.newBuilder();
                builder.addAllResult(batch);
                responseObserver.onNext(builder.build());
            }
            if (!it.hasNext()) {
                break;
            }
        }
        responseObserver.onCompleted();
    }

    public void getGraphMeta(com.lightgraph.raft.proto.GetGraphMetaRequest request,
            io.grpc.stub.StreamObserver<com.lightgraph.raft.proto.GetGraphMetaResponse> responseObserver) {
        GetGraphMetaResponse.Builder builder = GetGraphMetaResponse.newBuilder();
        String graph = request.getGraph();
        GraphMeta meta = metaRpcService.getGraphMeta(graph);
        builder.setResult(ByteString.copyFrom(meta.getBytes()));
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    public void listGraphMeta(com.lightgraph.raft.proto.ListGraphMetaRequest request,
            io.grpc.stub.StreamObserver<com.lightgraph.raft.proto.ListGraphMetaResponse> responseObserver) {
        ListGraphMetaResponse.Builder builder = ListGraphMetaResponse.newBuilder();
        List<GraphMeta> metas = metaRpcService.listGraphMeta();
        metas.forEach(m -> builder.addResult(ByteString.copyFrom(m.getBytes())));
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void listLabelMeta(com.lightgraph.raft.proto.ListLabelMetaRequest request,
            io.grpc.stub.StreamObserver<com.lightgraph.raft.proto.ListLabelMetaResponse> responseObserver) {
        ListLabelMetaResponse.Builder builder = ListLabelMetaResponse.newBuilder();
        List<LabelMeta> metas = metaRpcService
                .listLabelMeta(request.getGraph(), MetaType.valueOf(request.getMetaType()));
        metas.forEach(m -> builder.addResult(ByteString.copyFrom(m.getBytes())));
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getNodeVersion(com.lightgraph.raft.proto.GetNodeVersionRequest request,
            io.grpc.stub.StreamObserver<com.lightgraph.raft.proto.GetNodeVersionResponse> responseObserver) {
        GetNodeVersionResponse.Builder builder = GetNodeVersionResponse.newBuilder();
        long version = metaRpcService.getNodeVersion();
        builder.setVersion(version);
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void addVertexMeta(com.lightgraph.raft.proto.AddVertexMetaRequest request,
            io.grpc.stub.StreamObserver<com.lightgraph.raft.proto.AddVertexMetaResponse> responseObserver) {
        AddVertexMetaResponse.Builder builder = AddVertexMetaResponse.newBuilder();
        VertexMetaInfo info = VertexMetaInfo.getInstance(request.getVertexMeta().toByteArray(), VertexMetaInfo.class);
        boolean success = metaRpcService.addVertexMeta(info);
        builder.setSuccess(success);
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void addEdgeMeta(com.lightgraph.raft.proto.AddEdgeMetaRequest request,
            io.grpc.stub.StreamObserver<com.lightgraph.raft.proto.AddEdgeMetaResponse> responseObserver) {
        AddEdgeMetaResponse.Builder builder = AddEdgeMetaResponse.newBuilder();
        EdgeMetaInfo info = EdgeMetaInfo.getInstance(request.getEdgeMeta().toByteArray(), EdgeMetaInfo.class);
        boolean success = metaRpcService.addEdgeMeta(info);
        builder.setSuccess(success);
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getVertexMeta(com.lightgraph.raft.proto.GetVertexMetaRequest request,
            io.grpc.stub.StreamObserver<com.lightgraph.raft.proto.GetVertexMetaResponse> responseObserver) {
        GetVertexMetaResponse.Builder builder = GetVertexMetaResponse.newBuilder();
        VertexMeta meta = metaRpcService.getVertexMeta(request.getGraph(), request.getName());
        builder.setResult(ByteString.copyFrom(meta.getBytes()));
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getEdgeMeta(com.lightgraph.raft.proto.GetEdgeMetaRequest request,
            io.grpc.stub.StreamObserver<com.lightgraph.raft.proto.GetEdgeMetaResponse> responseObserver) {
        GetEdgeMetaResponse.Builder builder = GetEdgeMetaResponse.newBuilder();
        EdgeMeta meta = metaRpcService.getEdgeMeta(request.getGraph(), request.getName());
        builder.setResult(ByteString.copyFrom(meta.getBytes()));
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getLabelMetaById(com.lightgraph.raft.proto.GetLabelMetaByIdRequest request,
            io.grpc.stub.StreamObserver<com.lightgraph.raft.proto.GetLabelMetaByIdResponse> responseObserver) {
        GetLabelMetaByIdResponse.Builder builder = GetLabelMetaByIdResponse.newBuilder();
        LabelMeta meta = metaRpcService.getLabelMetaById(request.getId(), LabelType.valueOf(request.getLabelType()));
        builder.setResult(ByteString.copyFrom(meta.getBytes()));
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void addReplications(com.lightgraph.raft.proto.AddReplicationsRequest request,
            io.grpc.stub.StreamObserver<com.lightgraph.raft.proto.AddReplicationsResponse> responseObserver) {
        AddReplicationsResponse.Builder builder = AddReplicationsResponse.newBuilder();
        List<Replication> replications = new ArrayList<>();
        request.getReplicationsList()
                .forEach(r -> replications.add(Replication.getInstance(r.toByteArray(), Replication.class)));
        boolean ret = metaRpcService.addReplications(replications);
        builder.setSuccess(ret);
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateReplication(com.lightgraph.raft.proto.UpdateReplicationRequest request,
            io.grpc.stub.StreamObserver<com.lightgraph.raft.proto.UpdateReplicationResponse> responseObserver) {
        UpdateReplicationResponse.Builder builder = UpdateReplicationResponse.newBuilder();
        Replication replication = Replication.getInstance(request.getReplication().toByteArray(), Replication.class);
        boolean ret = metaRpcService.updateReplication(replication);
        builder.setSuccess(ret);
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }
}