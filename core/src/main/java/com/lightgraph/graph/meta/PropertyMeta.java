package com.lightgraph.graph.meta;

import com.lightgraph.graph.modules.storage.Key;

public class PropertyMeta extends ElementMeta {

    private DataType dataType;
    private Object defaultValue;

    public PropertyMeta() {
    }

    @Override
    protected Key key() {
        throw new RuntimeException("property not support this operation.");
    }

    public PropertyMeta(String name) {
        super(name, MetaType.PROPERTY);
    }

    public DataType getDataType() {
        return dataType;
    }

    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    public <T> T getDefaultValue() {
        return (T) defaultValue;
    }

    public <T> void setDefaultValue(T defaultValue) {
        this.defaultValue = defaultValue;
    }
}
