package com.lightgraph.graph.exception;

public class ReplicationNotExistException extends GraphException {


    public ReplicationNotExistException(String s) {
        super(s);
    }

    public ReplicationNotExistException(String s, Exception e) {
        super(s, e);
    }
}
