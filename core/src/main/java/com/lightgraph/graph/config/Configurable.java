package com.lightgraph.graph.config;

public interface Configurable {

    void set(String key, String value);

    <T> T get(String key, T defaultValue);
}
