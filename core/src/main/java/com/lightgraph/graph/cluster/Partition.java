package com.lightgraph.graph.cluster;

import com.lightgraph.graph.constant.GraphConstant;
import com.lightgraph.graph.exception.GraphException;
import com.lightgraph.graph.modules.consensus.ConsensusGroup;

import java.util.*;

public class Partition extends ConsensusGroup<Replication> {

    private String graph;
    private Integer partitionIndex;

    public Partition(String graph, Integer partitionIndex) {
        if (graph == null || partitionIndex == null) {
            throw new GraphException("graph or partitionIndex must not be null!");
        }
        this.graph = graph;
        this.partitionIndex = partitionIndex;
        this.groupName = getName();
    }

    public String getGraph() {
        return graph;
    }

    public void setGraph(String graph) {
        this.graph = graph;
    }

    public Map<Integer, Replication> getReplications() {
        return instances;
    }

    public Integer getPartitionIndex() {
        return partitionIndex;
    }

    public void setReplications(Map<Integer, Replication> replications) {
        this.instances = replications;
    }

    public void addReplication(Replication replication) {
        instances.put(replication.getReplicationIndex(), replication);
    }

    public String getName() {
        String g = new String(Base64.getEncoder().encode(graph.getBytes()));
        String uniqName = g + GraphConstant.SPLIT_ARRAY_TOKEN + partitionIndex;
        return new String(Base64.getEncoder().encode((uniqName).getBytes()));
    }

    public static Partition valueOf(String name) {
        String uniqName = new String(Base64.getDecoder().decode(name.getBytes()));
        String[] items = uniqName.split(GraphConstant.SPLIT_ARRAY_TOKEN);
        String graph = new String(Base64.getDecoder().decode(items[0]));
        Integer partitionIndex = Integer.valueOf(items[1]);
        Partition partition = new Partition(graph, partitionIndex);
        return partition;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Partition other = (Partition) obj;
        return Objects.equals(graph, other.graph)
                && Objects.equals(partitionIndex, other.partitionIndex)
                && Objects.equals(instances, other.instances);
    }

    @Override
    public String toString() {
        return graph + "_" + partitionIndex;
    }
}
