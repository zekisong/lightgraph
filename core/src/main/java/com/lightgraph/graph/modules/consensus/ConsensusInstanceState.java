package com.lightgraph.graph.modules.consensus;

import com.lightgraph.graph.exception.GraphException;

public enum ConsensusInstanceState {
    LEADER((byte) 0),
    FOLLOWER((byte) 1),
    UNSTABLE((byte) 2);

    private byte value;

    ConsensusInstanceState(byte value) {
        this.value = value;
    }

    public byte getValue() {
        return value;
    }

    public static ConsensusInstanceState valueOf(byte value) {
        switch (value) {
            case 0:
                return LEADER;
            case 1:
                return FOLLOWER;
            case 2:
                return UNSTABLE;
            default:
                throw new GraphException(String.format("ConsensusInstanceState:%d not exits!", value));
        }
    }
}