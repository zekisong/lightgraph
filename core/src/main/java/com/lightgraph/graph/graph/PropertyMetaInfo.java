package com.lightgraph.graph.graph;

import com.lightgraph.graph.meta.DataType;
import com.lightgraph.graph.writable.Writable;

public class PropertyMetaInfo extends Writable {

    private String name;
    private DataType dataType;
    private Object defaultValue;

    public PropertyMetaInfo() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public DataType getDataType() {
        return dataType;
    }

    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
    }
}
