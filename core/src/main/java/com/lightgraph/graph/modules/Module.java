package com.lightgraph.graph.modules;

import com.lightgraph.graph.config.Configurable;

public interface Module {

    void setup(Configurable config);

    boolean start();

    boolean shutdown();

    String moduleInfo();
}
