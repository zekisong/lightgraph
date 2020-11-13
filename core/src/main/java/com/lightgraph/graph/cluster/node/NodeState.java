package com.lightgraph.graph.cluster.node;

import com.lightgraph.graph.exception.GraphException;

public enum NodeState {
    INIT((byte) 0),
    HEALTH((byte) 1),
    UNHEALTH((byte) 2),
    DEAD((byte) 3);

    private byte value;

    NodeState(byte value) {
        this.value = value;
    }

    public byte getValue() {
        return value;
    }

    public static NodeState valueOf(byte value) {
        switch (value) {
            case 0:
                return INIT;
            case 1:
                return HEALTH;
            case 2:
                return UNHEALTH;
            case 3:
                return DEAD;
            default:
                throw new GraphException(String.format("NodeState:%d not exits!", value));
        }
    }
}