package com.lightgraph.graph.exception;


public class GraphException extends RuntimeException {

    private ErrorCode errorcode;

    public GraphException() {
    }

    public GraphException(String s) {
        super(s);
    }

    public GraphException(String s, Exception e) {
        super(s, e);
    }
}
