package com.lightgraph.graph.modules.rpc;


import com.lightgraph.graph.modules.storage.KeyValue;

import java.util.List;

public interface StorageRpcService {

    boolean put(String graph, KeyValue keyValue);

    List<KeyValue> scan(String graph, KeyValue keyValue);

    KeyValue get(String graph, KeyValue keyValue);
}
