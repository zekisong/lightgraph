package network;

import com.lightgraph.graph.config.Configurable;
import com.lightgraph.graph.modules.NetworkModule;
import com.lightgraph.graph.cluster.node.Node;
import io.grpc.ServerBuilder;

import java.io.IOException;


/**
 * Shared with rpc & raft
 */
public class NetworkModuleImpl implements NetworkModule {
    private ServerBuilder builder;

    @Override
    public void setup(Configurable config) {
        builder = ServerBuilder.forPort(Node.myself().getPort());
    }

    @Override
    public boolean start() {
        try {
            builder.build().start();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean shutdown() {
        return false;
    }

    @Override
    public String moduleInfo() {
        return "grpc impl of network module, clazz:" + this.getClass().getName();
    }

    @Override
    public ServerBuilder getBuilder() {
        return this.builder;
    }
}
