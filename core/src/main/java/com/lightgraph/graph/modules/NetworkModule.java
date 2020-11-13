package com.lightgraph.graph.modules;

public interface NetworkModule extends Module {
    <T> T getBuilder();
}
