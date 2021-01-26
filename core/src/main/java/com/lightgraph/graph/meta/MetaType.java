package com.lightgraph.graph.meta;

import com.lightgraph.graph.exception.GraphException;

public enum MetaType {
    PROPERTY((byte) 'p'),
    VERTEX((byte) 'v'),
    EDGE((byte) 'e'),
    GRAPH((byte) 'g'),
    INDEX((byte) 'i');

    private byte value;

    MetaType(byte value) {
        this.value = value;
    }

    public byte getValue() {
        return value;
    }

    public static MetaType valueOf(byte value) {
        switch (value) {
            case 'p':
                return PROPERTY;
            case 'v':
                return VERTEX;
            case 'e':
                return EDGE;
            case 'g':
                return GRAPH;
            case 'i':
                return INDEX;
            default:
                throw new GraphException(String.format("MetaType:%d not exits!", value));
        }
    }
}
