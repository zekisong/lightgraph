package com.lightgraph.graph.modules.consensus;


import com.lightgraph.graph.writable.Writable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConsensusGroup<T extends ConsensusInstance> extends Writable {

    protected String groupName;
    protected Map<Integer, T> instances = new ConcurrentHashMap<>();
    protected int size;

    public String getGroupName() {
        return groupName;
    }

    public Map<Integer, T> getInstances() {
        return instances;
    }

    public int getSize() {
        return instances.size();
    }
}
