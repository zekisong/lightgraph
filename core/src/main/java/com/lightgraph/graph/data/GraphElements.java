package com.lightgraph.graph.data;

import com.lightgraph.graph.meta.Meta;

public abstract class GraphElements<M extends Meta> {

    protected M meta;

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
