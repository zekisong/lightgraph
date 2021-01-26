package com.lightgraph.graph.data;

import com.lightgraph.graph.meta.PropertyMeta;

public class Property extends GraphElements<PropertyMeta> {

    private Long pid;
    private Object value;

    public Property() {
    }

    public Property(PropertyMeta meta) {
        super(meta);
        this.pid = meta.getId();
    }

    public <T> Property(PropertyMeta meta, T value) {
        this(meta);
        this.value = value;
        this.pid = meta.getId();
    }

    public Long getPid() {
        return pid;
    }

    public <T> T getValue() {
        if (value == null && meta.getDefaultValue() != null) {
            return meta.getDefaultValue();
        }
        return (T) value;
    }

    public <T> void setValue(T value) {
        this.value = value;
    }
}
