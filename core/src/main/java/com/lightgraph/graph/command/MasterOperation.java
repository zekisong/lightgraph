package com.lightgraph.graph.command;

import com.lightgraph.graph.exception.GraphException;

public enum MasterOperation {
    SET_MASTER_LEADER((byte) 1),
    ADD_NODE((byte) 2),
    REMOVE_NODE((byte) 3),
    CREATE_GRAPH((byte) 4),
    DROP_GRAPH((byte) 5),
    ADD_REPLICATION((byte) 6),
    REMOVE_REPLICATION((byte) 7),
    UPDATE_REPLICATION((byte) 8),
    UPDATE_NODE_VERSION((byte) 9),
    DO_SNAPSHOT((byte) 10),
    ADD_VERTEX_META((byte) 11),
    ADD_EDGE_META((byte) 12),
    ADD_PROPERTY_META((byte) 13);

    private byte OP;

    MasterOperation(byte OP) {
        this.OP = OP;
    }

    public byte getOP() {
        return OP;
    }

    public static MasterOperation valueOf(byte op) {
        switch (op) {
            case 1:
                return SET_MASTER_LEADER;
            case 2:
                return ADD_NODE;
            case 3:
                return REMOVE_NODE;
            case 4:
                return CREATE_GRAPH;
            case 5:
                return DROP_GRAPH;
            case 6:
                return ADD_REPLICATION;
            case 7:
                return REMOVE_REPLICATION;
            case 8:
                return UPDATE_REPLICATION;
            case 9:
                return UPDATE_NODE_VERSION;
            case 10:
                return DO_SNAPSHOT;
            case 11:
                return ADD_VERTEX_META;
            case 12:
                return ADD_EDGE_META;
            case 13:
                return ADD_PROPERTY_META;
            default:
                throw new GraphException("master not support this operation!");
        }
    }

    public static boolean shouldSync(MasterOperation operation) {
        if (operation == MasterOperation.CREATE_GRAPH
                || operation == MasterOperation.ADD_PROPERTY_META
                || operation == MasterOperation.ADD_VERTEX_META
                || operation == MasterOperation.ADD_EDGE_META) {
            return false;
        }
        return true;
    }
}