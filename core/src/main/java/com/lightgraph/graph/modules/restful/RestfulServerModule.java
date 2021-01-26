package com.lightgraph.graph.modules.restful;

import com.lightgraph.graph.modules.Module;

public interface RestfulServerModule extends Module {

    RestfulServerHandler getRestfulServiceHandler();
}