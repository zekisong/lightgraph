package com.lightgraph.graph.cluster.node;

import com.lightgraph.graph.exception.GraphException;

public enum NodeType {
    MASTER((byte) 0),
    DATA((byte) 1);

    private byte value;

    NodeType(byte value) {
        this.value = value;
    }

    public byte getValue() {
        return value;
    }

    public static NodeType valueOf(byte value) {
        switch (value) {
            case 0:
                return MASTER;
            case 1:
                return DATA;
            default:
                throw new GraphException(String.format("NodeType:%d not exits!", value));
        }
    }
}