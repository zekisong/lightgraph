package com.lightgraph.graph.cluster.manager;

import com.lightgraph.graph.command.MetaCommandRunner;
import com.lightgraph.graph.exception.GraphException;
import com.lightgraph.graph.cluster.Replication;
import com.lightgraph.graph.graph.EdgeMetaInfo;
import com.lightgraph.graph.graph.VertexMetaInfo;
import com.lightgraph.graph.meta.EdgeMeta;
import com.lightgraph.graph.meta.VertexMeta;
import com.lightgraph.graph.meta.cluster.GraphMeta;
import com.lightgraph.graph.modules.consensus.ConsensusIO;
import com.lightgraph.graph.modules.consensus.WriteFuture;
import com.lightgraph.graph.server.Server;
import com.lightgraph.graph.cluster.node.Node;
import com.lightgraph.graph.settings.GraphSetting;
import com.lightgraph.graph.storage.StorageManager;
import com.lightgraph.graph.timewheel.Task;
import com.lightgraph.graph.timewheel.TimeWheel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class DataNodeManager extends NodeManager {

    private static Log LOG = LogFactory.getLog(DataNodeManager.class);
    private LinkedBlockingQueue<byte[]> deltaQueue = new LinkedBlockingQueue();
    private ConsensusIO consensusIO;

    public DataNodeManager(StorageManager storage, Server server) {
        super(storage, server);
        consensusIO = new ConsensusIOImpl();
        commandRunner = new MetaCommandRunner(this, null);
        Thread thread = new Thread(commandRunner);
        thread.setDaemon(true);
        thread.setName("datanode-command-runner");
        thread.start();
        new TimeWheel(1000, 1).addTask(new UpdateVersionTask(), false);
    }

    @Override
    public boolean joinToCluster(Node node) {
        throw new GraphException("should not reach here!");
    }

    @Override
    public boolean createGraph(GraphSetting setting) {
        throw new GraphException("should not reach here!");
    }

    @Override
    public boolean createGraphInner(GraphSetting setting) {
        throw new GraphException("should not reach here!");
    }

    @Override
    public synchronized boolean addReplications(List<Replication> replications) {
        throw new GraphException("should not reach here!");
    }

    @Override
    public synchronized List<byte[]> updateContext(long version) {
        throw new GraphException("should not reach here!");
    }

    @Override
    public synchronized boolean updateReplication(Replication replication) {
        throw new GraphException("should not reach here!");
    }

    @Override
    public GraphMeta getGraphMeta(String graph) {
        throw new GraphException("should not reach here!");
    }

    @Override
    public List<GraphMeta> listGraphMeta() {
        throw new GraphException("should not reach here!");
    }

    @Override
    public boolean addVertexMeta(VertexMetaInfo vertexMetaInfo) {
        throw new GraphException("should not reach here!");
    }

    @Override
    public boolean addEdgeMeta(EdgeMetaInfo edgeMetaInfo) {
        throw new GraphException("should not reach here!");
    }

    @Override
    public VertexMeta getVertexMeta(String graph, String name) {
        throw new GraphException("should not reach here!");
    }

    @Override
    public EdgeMeta getEdgeMeta(String graph, String name) {
        throw new GraphException("should not reach here!");
    }

    @Override
    public boolean addVertexMetaInner(VertexMetaInfo vertexMeta) {
        throw new GraphException("should not reach here!");
    }

    @Override
    public boolean addEdgeMetaInner(EdgeMetaInfo edgeMetaInfo) {
        throw new GraphException("should not reach here!");
    }

    @Override
    public synchronized long getNodeVersion() {
        return context.getVersion();
    }

    @Override
    public synchronized boolean joinToClusterInner(Node node) {
        context.getNodes().put(node.getName(), node);
        this.context.incrVersion();
        return true;
    }

    @Override
    public synchronized void setLeaderInner(Node leader) {
        context.setLeader(leader);
        this.context.incrVersion();
    }

    @Override
    public synchronized boolean addReplicationInner(Replication replication) {
        context.addReplication(replication);
        context.incrVersion();
        if (Node.myself().equals(replication.getLocation())) {
            storage.createStorageIfNotExist(replication);
        }
        return true;
    }

    @Override
    public synchronized boolean updateReplicationInner(Replication replication) {
        context.updateReplication(replication);
        context.incrVersion();
        return true;
    }

    @Override
    public ConsensusIO getConsensusIO() {
        return consensusIO;
    }

    class ConsensusIOImpl implements ConsensusIO {

        @Override
        public WriteFuture write(byte[] record) {
            throw new RuntimeException("data node manager does't support write operation!");
        }

        @Override
        public byte[] read() {
            try {
                return deltaQueue.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return new byte[0];
        }
    }

    class UpdateVersionTask implements Task {

        @Override
        public String description() {
            return "update version task";
        }

        @Override
        public Object doTask() {
            List<byte[]> patchs = null;
            try {
                if (deltaQueue.isEmpty()) {
                    patchs = server.getMaster().updateContext(context.getVersion());
                }
            } catch (Throwable e) {
                LOG.warn("pull deltas from master failed, may be master not prepare!", e);
            }
            if (patchs != null && patchs.size() > 0) {
                for (byte[] patch : patchs) {
                    try {
                        deltaQueue.put(patch);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            return null;
        }
    }
}
