package com.lightgraph.graph.timewheel;

public interface Task<T> {
    String description();

    T doTask();
}
