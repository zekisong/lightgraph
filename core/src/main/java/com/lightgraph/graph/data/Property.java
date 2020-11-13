package com.lightgraph.graph.data;


import com.lightgraph.graph.meta.DataType;
import com.lightgraph.graph.meta.PropertyMeta;
import com.lightgraph.graph.utils.DataTypeUtils;
import com.lightgraph.graph.writable.Sizeable;
import com.lightgraph.graph.writable.Writable;

public class Property extends GraphElements<PropertyMeta> implements Writable, Sizeable {
    private Object value;

    public Property(PropertyMeta meta) {
        super(meta);
    }

    public <T> Property(PropertyMeta meta, T value) {
        this(meta);
        this.value = value;
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

    @Override
    public int size() {
        int valueSize;
        if (value == null) {
            value = meta.getDefaultValue();
        }
        if (meta.getDataType() == DataType.STRING) {
            valueSize = value.toString().length();
        } else {
            valueSize = DataTypeUtils.sizeOf(meta.getDataType());
        }
        return 0;
    }

    @Override
    public byte[] getBytes() {
        return new byte[0];
    }
}
