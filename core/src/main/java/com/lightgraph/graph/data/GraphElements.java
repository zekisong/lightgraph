package com.lightgraph.graph.data;

import com.lightgraph.graph.meta.Meta;
import com.lightgraph.graph.writable.Writable;

public abstract class GraphElements<M extends Meta> extends Writable {

    protected transient M meta;

    public GraphElements() {
    }

    public GraphElements(M meta) {
        this.meta = meta;
    }

    public M getMeta() {
        return meta;
    }

    public void setMeta(M meta) {
        this.meta = meta;
    }
}
