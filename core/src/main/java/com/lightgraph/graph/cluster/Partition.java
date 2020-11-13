package com.lightgraph.graph.cluster;

import com.lightgraph.graph.constant.GraphConstant;
import com.lightgraph.graph.exception.GraphException;
import com.lightgraph.graph.modules.consensus.ConsensusGroup;
import com.lightgraph.graph.utils.ByteUtils;
import com.lightgraph.graph.writable.Sizeable;
import com.lightgraph.graph.writable.Writable;

import java.util.*;

public class Partition extends ConsensusGroup<Replication> implements Writable, Sizeable {
    private String graph;
    private Integer partitionIndex;

    public Partition(String graph, Integer partitionIndex) {
        if (graph == null || partitionIndex == null)
            throw new GraphException("graph or partitionIndex must not be null!");
        this.graph = graph;
        this.partitionIndex = partitionIndex;
        this.groupName = getName();
    }

    public Partition(byte[] bytes) {
        int pos = ByteUtils.RESERVED_BYTE_SIZE＿FOR_TX;
        int groupLen = ByteUtils.getInt(bytes, pos);
        pos = pos + ByteUtils.SIZE_INT;
        int instanceCount = ByteUtils.getInt(bytes, pos);
        pos = pos + ByteUtils.SIZE_INT;
        List<Integer> instances = new ArrayList<>();
        for (int i = 0; i < instanceCount; i++) {
            int instanceLen = ByteUtils.getInt(bytes, pos);
            pos = pos + ByteUtils.SIZE_INT;
            instances.add(instanceLen);
        }
        int graphLen = ByteUtils.getInt(bytes, pos);
        pos = pos + ByteUtils.SIZE_INT;
        this.groupName = ByteUtils.getString(bytes, pos, groupLen);
        pos = pos + groupLen;
        for (int len : instances) {
            byte[] replicationB = ByteUtils.getBytes(bytes, pos, len);
            Replication replication = new Replication(replicationB);
            this.instances.put(replication.getReplicationIndex(), replication);
            pos = pos + len;
        }
        this.graph = ByteUtils.getString(bytes, pos, graphLen);
        pos = pos + graphLen;
        this.partitionIndex = ByteUtils.getInt(bytes, pos);
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
        if (this == obj)
            return true;
        if (obj == null || getClass() != obj.getClass())
            return false;
        Partition other = (Partition) obj;
        return Objects.equals(graph, other.graph)
                && Objects.equals(partitionIndex, other.partitionIndex)
                && Objects.equals(instances, other.instances);
    }

    @Override
    public String toString() {
        return graph + "_" + partitionIndex;
    }

    @Override
    public int size() {
        int replicationHeapSize = 0;
        for (Replication replication : instances.values()) {
            replicationHeapSize = replicationHeapSize + replication.size();
        }
        return ByteUtils.RESERVED_BYTE_SIZE＿FOR_TX
                + ByteUtils.SIZE_INT  //group size
                + ByteUtils.SIZE_INT  //instance count
                + ByteUtils.SIZE_INT * instances.size() //replicationHeap size
                + ByteUtils.SIZE_INT  //graph size
                + this.groupName.length()
                + replicationHeapSize
                + graph.length()
                + ByteUtils.SIZE_INT;  //partition index
    }

    @Override
    public byte[] getBytes() {
        int size = size();
        byte[] data = new byte[size];
        int pos = ByteUtils.RESERVED_BYTE_SIZE＿FOR_TX;
        pos = ByteUtils.putInt(data, pos, this.groupName.length());
        pos = ByteUtils.putInt(data, pos, this.instances.size());
        for (Replication replication : instances.values()) {
            pos = ByteUtils.putInt(data, pos, replication.size());
        }
        pos = ByteUtils.putInt(data, pos, this.graph.length());
        pos = ByteUtils.putString(data, pos, this.groupName);
        for (Replication replication : instances.values()) {
            pos = ByteUtils.putBytes(data, pos, replication.getBytes());
        }
        pos = ByteUtils.putString(data, pos, this.graph);
        ByteUtils.putInt(data, pos, partitionIndex);
        return data;
    }
}
