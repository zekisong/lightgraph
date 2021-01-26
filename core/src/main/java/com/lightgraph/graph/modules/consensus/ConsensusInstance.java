package com.lightgraph.graph.modules.consensus;

import com.lightgraph.graph.writable.Writable;
import com.lightgraph.graph.cluster.node.Node;

import java.util.Collection;
import java.util.Objects;

public class ConsensusInstance<T extends ConsensusGroup> extends Writable {

    protected String description;
    protected transient T group;
    protected String instanceName;
    protected Node location;
    protected int groupSize = -1;
    protected volatile ConsensusInstanceState state = ConsensusInstanceState.UNSTABLE;

    public String getInstanceName() {
        return instanceName;
    }

    public Node getLocation() {
        return location;
    }

    public void setLocation(Node location) {
        this.location = location;
    }

    public Iterable<ConsensusInstance> getOtherMember() {
        Collection<ConsensusInstance> instances = this.group.getInstances().values();
        return () -> instances.stream().filter(instance -> !instance.equals(this)).iterator();
    }

    public void setGroup(T group) {
        this.group = group;
    }

    public void setGroupSize(int groupSize) {
        this.groupSize = groupSize;
    }

    public boolean isReady() {
        return groupSize != -1;
    }

    public int getGroupSize() {
        return groupSize;
    }

    public ConsensusInstanceState getState() {
        return state;
    }

    public void setState(ConsensusInstanceState state) {
        this.state = state;
    }

    public String getDescription() {
        return description;
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
        return Objects.equals(this.instanceName, other.instanceName);
    }

    @Override
    public String toString() {
        return String.format("instanceName:%s,node:%s", instanceName, location.getName());
    }

    @Override
    public int hashCode() {
        int hash = 17;
        hash = 31 * hash + instanceName.hashCode();
        return hash;
    }
}