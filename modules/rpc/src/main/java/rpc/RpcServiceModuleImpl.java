package rpc;

import com.lightgraph.graph.config.Configurable;
import com.lightgraph.graph.modules.NetworkModule;
import com.lightgraph.graph.modules.rpc.StorageRpcService;
import com.lightgraph.graph.modules.rpc.MetaRpcService;
import com.lightgraph.graph.modules.rpc.RpcServiceModule;
import com.lightgraph.graph.cluster.node.Node;
import com.lightgraph.raft.proto.RpcServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RpcServiceModuleImpl implements RpcServiceModule {

    private Map<Node, RpcClient> handlerMap = new ConcurrentHashMap<>();
    ServerBuilder builder;

    @Override
    public void setup(Configurable config) {

    }

    @Override
    public void bindNetwork(NetworkModule networkModule) {
        builder = networkModule.getBuilder();
    }

    @Override
    public void bindRpcService(MetaRpcService metaRpcService, StorageRpcService dataRpcService) {
        builder.addService(new RpcServer(metaRpcService, dataRpcService));
    }

    @Override
    public MetaRpcService getRpcServiceHandler(Node node) {
        if (handlerMap.containsKey(node)) {
            return handlerMap.get(node);
        } else {
            ManagedChannel channel = ManagedChannelBuilder.forAddress(node.getAddr(), node.getPort())
                    .usePlaintext(true)
                    .build();
            RpcServiceGrpc.RpcServiceBlockingStub syncStub = RpcServiceGrpc.newBlockingStub(channel);
            RpcServiceGrpc.RpcServiceStub asyncStub = RpcServiceGrpc.newStub(channel);
            RpcClient handler = new RpcClient(syncStub, asyncStub);
            handlerMap.put(node, handler);
            return handler;
        }
    }

    @Override
    public boolean start() {
        return false;
    }

    @Override
    public boolean shutdown() {
        return false;
    }

    @Override
    public String moduleInfo() {
        return null;
    }


}
