package com.lightgraph.graph.cluster.context;

import com.lightgraph.graph.cluster.Partition;
import com.lightgraph.graph.cluster.Replication;
import com.lightgraph.graph.cluster.node.Node;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ClusterContext {
    private static Log LOG = LogFactory.getLog(ClusterContext.class);
    private volatile Node leader;
    private Map<String, Node> nodes = new ConcurrentHashMap<>();
    private Map<String, Map<Integer, Partition>> routting = new ConcurrentHashMap<>();
    private volatile AtomicLong version = new AtomicLong(0);

    public boolean isEmpty() {
        if (leader == null && nodes.size() == 0 && routting.size() == 0)
            return true;
        else
            return false;
    }

    public Node getLeader() {
        return leader;
    }

    public void setLeader(Node leader) {
        this.leader = leader;
    }

    public Map<String, Node> getNodes() {
        return nodes;
    }

    public Map<String, Map<Integer, Partition>> getRoutting() {
        return routting;
    }

    public long getVersion() {
        return version.get();
    }

    public void setVersion(long version) {
        this.version.set(version);
    }

    public void incrVersion() {
        version.incrementAndGet();
    }

    public void addReplication(Replication replication) {
        String graph = replication.getGraphName();
        Partition partition;
        Integer partitionIndex = replication.getPartitionIndex();
        if (routting.containsKey(graph)) {
            Map<Integer, Partition> partitions = routting.get(graph);
            if (partitions.containsKey(partitionIndex)) {
                partition = partitions.get(partitionIndex);
                partition.addReplication(replication);
            } else {
                partition = new Partition(graph, partitionIndex);
                partition.addReplication(replication);
                partitions.put(partitionIndex, partition);
            }
        } else {
            partition = new Partition(graph, partitionIndex);
            partition.addReplication(replication);
            Map partitions = new ConcurrentHashMap();
            partitions.put(partitionIndex, partition);
            routting.put(graph, partitions);
        }
        replication.setGroup(partition);
    }

    public void updateReplication(Replication replication) {
        Integer partitionIndex = replication.getPartitionIndex();
        String graph = replication.getGraphName();
        Integer replicationIndex = replication.getReplicationIndex();
        try {
            routting.get(graph)
                    .get(partitionIndex)
                    .getReplications()
                    .get(replicationIndex)
                    .setState(replication.getState());
        } catch (Exception e) {
            LOG.error("update replication failed!\t" + routting.keySet() + "\t" + graph + "\t" + routting.get(graph
            ), e);
            throw e;
        }
    }
}