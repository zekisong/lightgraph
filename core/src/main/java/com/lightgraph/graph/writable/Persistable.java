package com.lightgraph.graph.writable;

import com.lightgraph.graph.modules.storage.KeyValue;
import java.util.List;

public interface Persistable {

    List<KeyValue> toMutations();
}
