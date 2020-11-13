package com.lightgraph.graph.graph;

import com.lightgraph.graph.meta.DataType;
import com.lightgraph.graph.utils.ByteUtils;
import com.lightgraph.graph.utils.DataTypeUtils;
import com.lightgraph.graph.writable.Sizeable;
import com.lightgraph.graph.writable.Writable;

public class PropertyMetaInfo implements Writable, Sizeable {
    private String name;
    private DataType dataType;
    private Object defaultValue;

    public PropertyMetaInfo() {
    }

    public PropertyMetaInfo(byte[] bytes) {
        int pos = ByteUtils.RESERVED_BYTE_SIZE＿FOR_TX;
        int nameSize = ByteUtils.getInt(bytes, pos);
        pos = pos + ByteUtils.SIZE_INT;
        name = ByteUtils.getString(bytes, pos, nameSize);
        pos = pos + nameSize;
        dataType = DataType.valueOf(ByteUtils.getByte(bytes, pos));
        pos = pos + ByteUtils.SIZE_BYTE;
        switch (dataType) {
            case LONG:
            case FLOAT:
            case DOUBLE:
            case TIMESTAMP:
                defaultValue = ByteUtils.getLong(bytes, pos);
                break;
            case STRING:
                int len = ByteUtils.getInt(bytes, pos);
                pos = pos + ByteUtils.SIZE_INT;
                defaultValue = ByteUtils.getString(bytes, pos, len);
                break;
            case INTEGER:
                defaultValue = ByteUtils.getInt(bytes, pos);
                break;
            default:
                break;
        }
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

    @Override
    public int size() {
        int defaultValueSize;
        if (dataType == DataType.STRING) {
            defaultValueSize = defaultValue.toString().length();
        } else {
            defaultValueSize = DataTypeUtils.sizeOf(dataType);
        }
        return ByteUtils.RESERVED_BYTE_SIZE＿FOR_TX
                + ByteUtils.SIZE_INT
                + name.length()
                + ByteUtils.SIZE_BYTE
                + ByteUtils.SIZE_INT
                + defaultValueSize;
    }

    @Override
    public byte[] getBytes() {
        int size = size();
        byte[] data = new byte[size];
        int pos = ByteUtils.RESERVED_BYTE_SIZE＿FOR_TX;
        pos = ByteUtils.putInt(data, pos, name.length());
        pos = ByteUtils.putString(data, pos, name);
        pos = ByteUtils.put(data, pos, dataType.getValue());
        switch (dataType) {
            case LONG:
            case FLOAT:
            case DOUBLE:
            case TIMESTAMP:
                pos = ByteUtils.putInt(data, pos, ByteUtils.SIZE_LONG);
                pos = ByteUtils.putLong(data, pos, (long) defaultValue);
                break;
            case STRING:
                pos = ByteUtils.putInt(data, pos, defaultValue.toString().length());
                pos = ByteUtils.putString(data, pos, (String) defaultValue);
                break;
            case INTEGER:
                pos = ByteUtils.putInt(data, pos, ByteUtils.SIZE_INT);
                pos = ByteUtils.putInt(data, pos, (int) defaultValue);
                break;
            default:
                break;
        }
        return data;
    }
}
