package com.lightgraph.graph.modules.rpc;


import com.lightgraph.graph.cluster.Replication;
import com.lightgraph.graph.modules.storage.Batch;
import com.lightgraph.graph.modules.storage.Key;
import com.lightgraph.graph.modules.storage.KeyValue;

import java.util.Iterator;

public interface StorageRpcService {

    boolean put(String graph, KeyValue keyValue);

    boolean batchPut(String graph, Replication replication, Batch batch);

    Iterator<KeyValue> scan(String graph, Key start);

    KeyValue get(String graph, Key key);
}
