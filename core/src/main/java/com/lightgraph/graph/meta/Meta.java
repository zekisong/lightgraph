package com.lightgraph.graph.meta;


import com.lightgraph.graph.writable.Indexable;
import com.lightgraph.graph.writable.Persistable;

public interface Meta extends Persistable, Indexable {

    MetaState getState();

    String getName();
}
