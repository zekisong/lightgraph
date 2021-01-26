package com.lightgraph.graph.cluster;

import com.lightgraph.graph.constant.GraphConstant;
import com.lightgraph.graph.modules.consensus.ConsensusInstance;
import com.lightgraph.graph.cluster.node.Node;

import java.util.Base64;
import java.util.Objects;

public class Replication extends ConsensusInstance<Partition> {

    private Integer replicationIndex;

    public Replication() {

    }

    public Replication(Partition partition, Integer replicationIndex, Node location) {
        this(partition, replicationIndex);
        this.location = location;
    }

    public Replication(Partition partition, Integer replicationIndex) {
        this.group = partition;
        this.replicationIndex = replicationIndex;
        this.instanceName = getName();
        this.description = String.format("[graph:%s,partition:%d,replication:%d]", getGraphName(), getPartitionIndex(),
                getReplicationIndex());
    }

    public Node getLocation() {
        return location;
    }

    public void setLocation(Node location) {
        this.location = location;
    }

    public String getGraphName() {
        String uniqName = new String(Base64.getDecoder().decode(this.instanceName.getBytes()));
        String[] items = uniqName.split(GraphConstant.SPLIT_ARRAY_TOKEN);
        String graph = new String(Base64.getDecoder().decode(items[0]));
        return graph;
    }

    public Integer getPartitionIndex() {
        String uniqName = new String(Base64.getDecoder().decode(this.instanceName.getBytes()));
        String[] items = uniqName.split(GraphConstant.SPLIT_ARRAY_TOKEN);
        Integer partitionIndex = Integer.valueOf(items[1]);
        return partitionIndex;
    }

    public Integer getReplicationIndex() {
        return replicationIndex;
    }

    public String getName() {
        String g = new String(Base64.getEncoder().encode(this.group.getGraph().getBytes()));
        String uniqName =
                g + GraphConstant.SPLIT_ARRAY_TOKEN + this.group.getPartitionIndex() + GraphConstant.SPLIT_ARRAY_TOKEN
                        + replicationIndex;
        return new String(Base64.getEncoder().encode((uniqName).getBytes()));
    }

    public static Replication valueOf(String name) {
        String uniqName = new String(Base64.getDecoder().decode(name.getBytes()));
        String[] items = uniqName.split(GraphConstant.SPLIT_ARRAY_TOKEN);
        String graph = new String(Base64.getDecoder().decode(items[0]));
        Integer partitionIndex = Integer.valueOf(items[1]);
        Integer replicationIndex = Integer.valueOf(items[2]);
        Partition partition = new Partition(graph, partitionIndex);
        Replication replication = new Replication(partition, replicationIndex);
        return replication;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        ConsensusInstance other = (ConsensusInstance) obj;
        return Objects.equals(this.instanceName, other.getInstanceName()) && this.getState() == other.getState();
    }
}
