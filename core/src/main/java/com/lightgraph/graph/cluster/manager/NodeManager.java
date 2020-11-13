package com.lightgraph.graph.cluster.manager;

import com.lightgraph.graph.cluster.Partition;
import com.lightgraph.graph.cluster.Replication;
import com.lightgraph.graph.command.MetaCommandRunner;
import com.lightgraph.graph.graph.EdgeMetaInfo;
import com.lightgraph.graph.graph.VertexMetaInfo;
import com.lightgraph.graph.meta.PropertyMeta;
import com.lightgraph.graph.meta.VertexMeta;
import com.lightgraph.graph.modules.consensus.ConsensusIO;
import com.lightgraph.graph.modules.consensus.ConsensusInstanceState;
import com.lightgraph.graph.modules.rpc.MetaRpcService;
import com.lightgraph.graph.modules.rpc.ServiceType;
import com.lightgraph.graph.server.Server;
import com.lightgraph.graph.cluster.node.Node;
import com.lightgraph.graph.cluster.context.ClusterContext;
import com.lightgraph.graph.server.ServerService;
import com.lightgraph.graph.settings.GraphSetting;
import com.lightgraph.graph.storage.StorageManager;
import com.lightgraph.graph.utils.MurmurHashUtils;

import java.util.*;


public abstract class NodeManager implements MetaRpcService {
    protected volatile ClusterContext context = new ClusterContext();
    protected ServerService server;
    protected MetaCommandRunner commandRunner;
    protected List<byte[]> deltas = new ArrayList();
    protected StorageManager storage;

    public NodeManager(StorageManager storage, Server server) {
        this.storage = storage;
        this.server = server;
    }

    @Override
    public Node getLeader() {
        return context.getLeader();
    }

    @Override
    public long getNodeVersion() {
        return context.getVersion();
    }

    @Override
    public ServiceType getServiceType() {
        return ServiceType.META;
    }

    public Map<String, Node> getNodes() {
        return context.getNodes();
    }

    public Map<String, Map<Integer, Partition>> getRoutting() {
        return context.getRoutting();
    }

    public Replication getPartitionLeader(String graph, byte[] key) {
        if (getRoutting().get(graph) == null)
            return null;
        int partitions = getRoutting().get(graph).size();
        int partitionIndex = Math.abs(MurmurHashUtils.hash(key, 0, key.length, 0)) % partitions;
        Partition partition = context.getRoutting().get(graph).get(partitionIndex);
        for (Replication replication : partition.getReplications().values()) {
            if (replication.getState() == ConsensusInstanceState.LEADER) {
                return replication;
            }
        }
        return null;
    }

    public Set<Replication> getReplications(String graph, byte[] key) {
        Set<Replication> replications = new HashSet();
        if (getRoutting().get(graph) == null)
            return null;
        int partitions = getRoutting().get(graph).size();
        int partitionIndex = Math.abs(MurmurHashUtils.hash(key, 0, key.length, 0)) % partitions;
        Partition partition = context.getRoutting().get(graph).get(partitionIndex);
        replications.addAll(partition.getReplications().values());
        return replications;
    }

    public abstract boolean joinToClusterInner(Node node);

    public abstract void setLeaderInner(Node leader);

    public abstract boolean addReplicationInner(Replication replication);

    public abstract boolean updateReplicationInner(Replication replication);

    public abstract boolean createGraphInner(GraphSetting setting);

    public abstract boolean addVertexMetaInner(VertexMetaInfo vertexMetaInfo);

    public abstract boolean addEdgeMetaInner(EdgeMetaInfo edgeMetaInfo);

    public abstract ConsensusIO getConsensusIO();

}