package com.lightgraph.graph.modules.consensus;

import com.lightgraph.graph.writable.Sizeable;
import com.lightgraph.graph.writable.Writable;
import com.lightgraph.graph.cluster.node.Node;
import com.lightgraph.graph.utils.ByteUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Collection;
import java.util.Objects;

public class ConsensusInstance<T extends ConsensusGroup> implements Writable, Sizeable {
    protected String description;
    protected transient T group;
    protected String instanceName;
    protected Node location;
    protected int groupSize = -1;
    protected volatile ConsensusInstanceState state = ConsensusInstanceState.UNSTABLE;

    public ConsensusInstance() {
    }

    public ConsensusInstance(byte[] bytes) {
        int pos = ByteUtils.RESERVED_BYTE_SIZE＿FOR_TX;
        int descriptionLen = ByteUtils.getInt(bytes, pos);
        pos = pos + ByteUtils.SIZE_INT;
        int instanceNameLen = ByteUtils.getInt(bytes, pos);
        pos = pos + ByteUtils.SIZE_INT;
        int locationLen = ByteUtils.getInt(bytes, pos);
        pos = pos + ByteUtils.SIZE_INT;
        groupSize = ByteUtils.getInt(bytes, pos);
        pos = pos + ByteUtils.SIZE_INT;
        this.description = ByteUtils.getString(bytes, pos, descriptionLen);
        pos = pos + descriptionLen;
        this.instanceName = ByteUtils.getString(bytes, pos, instanceNameLen);
        pos = pos + instanceNameLen;
        byte[] nodeB = ByteUtils.getBytes(bytes, pos, locationLen);
        this.location = new Node(nodeB);
        pos = pos + locationLen;
        byte stateB = ByteUtils.getByte(bytes, pos);
        this.state = ConsensusInstanceState.valueOf(stateB);
    }

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
        if (this == obj)
            return true;
        if (obj == null)
            return false;
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

    @Override
    public int size() {
        return ByteUtils.RESERVED_BYTE_SIZE＿FOR_TX
                + ByteUtils.SIZE_INT
                + ByteUtils.SIZE_INT
                + ByteUtils.SIZE_INT
                + ByteUtils.SIZE_INT
                + this.description.length()
                + this.instanceName.length()
                + this.location.size()
                + ByteUtils.SIZE_BYTE;
    }

    @Override
    public byte[] getBytes() {
        int size = size();
        byte[] data = new byte[size];
        int pos = ByteUtils.RESERVED_BYTE_SIZE＿FOR_TX;
        pos = ByteUtils.putInt(data, pos, description.length());
        pos = ByteUtils.putInt(data, pos, instanceName.length());
        pos = ByteUtils.putInt(data, pos, location.size());
        pos = ByteUtils.putInt(data, pos, groupSize);
        pos = ByteUtils.putString(data, pos, description);
        pos = ByteUtils.putString(data, pos, instanceName);
        pos = ByteUtils.putBytes(data, pos, location.getBytes());
        ByteUtils.put(data, pos, state.getValue());
        return data;
    }
}