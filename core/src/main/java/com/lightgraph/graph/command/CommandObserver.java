package com.lightgraph.graph.command;

public interface CommandObserver {
    public void preRun();

    public void afterRun(byte[] data);
}
