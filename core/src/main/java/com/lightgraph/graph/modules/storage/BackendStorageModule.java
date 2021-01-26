package com.lightgraph.graph.modules.storage;

import com.lightgraph.graph.cluster.Replication;
import com.lightgraph.graph.modules.Module;

import java.util.List;

public interface BackendStorageModule extends Module {

    BackendStorageHandler createStorageIfNotExist(Replication replication);

    List<Replication> getExistReplication();
}
