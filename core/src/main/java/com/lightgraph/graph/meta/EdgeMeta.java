package com.lightgraph.graph.meta;

public class EdgeMeta extends LabelMeta {

    private Long subject;
    private Long object;

    public EdgeMeta() {
    }

    public EdgeMeta(String name) {
        super(name, MetaType.EDGE);
    }

    public Long getSubject() {
        return subject;
    }

    public void setSubject(Long subject) {
        this.subject = subject;
    }

    public Long getObject() {
        return object;
    }

    public void setObject(Long object) {
        this.object = object;
    }
}
