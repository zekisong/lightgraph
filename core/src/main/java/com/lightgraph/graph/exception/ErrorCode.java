package com.lightgraph.graph.exception;

public enum ErrorCode {
    INTERNAL_ERROR(400, "internal error"),
    NOT_FOUND(404, "not found");

    private int code;
    private String desc;

    ErrorCode(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }
}
