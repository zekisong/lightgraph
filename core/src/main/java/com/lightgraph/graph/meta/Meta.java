package com.lightgraph.graph.meta;

import com.lightgraph.graph.modules.storage.KeyValue;
import com.lightgraph.graph.writable.Sizeable;
import com.lightgraph.graph.writable.Writable;

import java.util.List;

public interface Meta extends Writable, Sizeable {
    MetaState getState();

    String getName();

    List<KeyValue> toKVS();
}
