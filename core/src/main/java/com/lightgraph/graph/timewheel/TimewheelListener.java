package com.lightgraph.graph.timewheel;

public interface TimewheelListener<T> {
    void onTaskFailed();

    void onTaskSuccess();

    void onTaskStart();

    void onTaskFinish();
}
