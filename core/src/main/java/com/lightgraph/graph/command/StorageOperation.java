package com.lightgraph.graph.command;

import com.lightgraph.graph.exception.GraphException;

public enum StorageOperation {
    PUT((byte) 1),
    DELETE((byte) 2);

    private byte OP;

    StorageOperation(byte OP) {
        this.OP = OP;
    }

    public byte getOP() {
        return OP;
    }

    public static StorageOperation valueOf(byte op) {
        switch (op) {
            case 1:
                return PUT;
            case 2:
                return DELETE;
            default:
                throw new GraphException("storage not support this operation:" + op);
        }
    }
}