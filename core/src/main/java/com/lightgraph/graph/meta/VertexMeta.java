package com.lightgraph.graph.meta;

public class VertexMeta extends LabelMeta {

    private String pk;

    public VertexMeta() {
    }

    public VertexMeta(String name) {
        super(name, MetaType.VERTEX);
    }

    public String getPK() {
        return pk;
    }

    public void setPK(String key) {
        this.pk = key;
    }
}
