package com.lightgraph.graph.meta;

import com.lightgraph.graph.constant.GraphConstant;
import com.lightgraph.graph.modules.storage.KeyValue;
import com.lightgraph.graph.utils.ByteUtils;
import com.lightgraph.graph.utils.DataTypeUtils;

import java.util.List;

public class PropertyMeta extends ElementMeta {
    private String graph;
    private DataType dataType;
    private Object defaultValue;

    public PropertyMeta(byte[] bytes) {
        super(bytes);
        int pos = super.size();
        int graphSize = ByteUtils.getInt(bytes, pos);
        pos = pos + ByteUtils.SIZE_INT;
        graph = ByteUtils.getString(bytes, pos, graphSize);
        pos = pos + graphSize;
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
                defaultValue = ByteUtils.getString(bytes, pos, len);
                break;
            case INTEGER:
                defaultValue = ByteUtils.getInt(bytes, pos);
                break;
            default:
                break;
        }
    }

    public PropertyMeta(List<KeyValue> keyValues) {
        super(keyValues);
        byte[] defaultV = null;
        for (KeyValue keyValue : keyValues) {
            byte[] key = keyValue.getKey();
            byte[] secKey = keyValue.getSecKey();
            byte[] value = keyValue.getValue();
            List<byte[]> keyItems = ByteUtils.split(key, GraphConstant.KEY_DELIMITER.getBytes());
            if (graph == null) {
                graph = new String(keyItems.get(0));
                type = MetaType.valueOf(keyItems.get(1)[0]);
                name = new String(keyItems.get(2));
            }
            switch (secKey[0]) {
                case 'i':
                    id = ByteUtils.getLong(value, 0);
                    break;
                case 't':
                    createTime = ByteUtils.getLong(value, 0);
                    break;
                case 'd':
                    byte type = ByteUtils.getByte(value, 0);
                    this.dataType = DataType.valueOf(type);
                    break;
                case 'v':
                    defaultV = value;
                    break;
            }
        }
        switch (dataType) {
            case LONG:
            case FLOAT:
            case DOUBLE:
            case TIMESTAMP:
                defaultValue = ByteUtils.getLong(defaultV, 0);
                break;
            case STRING:
                defaultValue = ByteUtils.getString(defaultV, 0, defaultV.length);
                break;
            case INTEGER:
                defaultValue = ByteUtils.getInt(defaultV, 0);
                break;
            default:
                break;
        }
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

    public String getGraph() {
        return graph;
    }

    public void setGraph(String graph) {
        this.graph = graph;
    }

    @Override
    public int size() {
        int defaultValueSize;
        if (dataType == DataType.STRING) {
            defaultValueSize = defaultValue.toString().length();
        } else {
            defaultValueSize = DataTypeUtils.sizeOf(dataType);
        }
        return super.size()
                + ByteUtils.SIZE_INT
                + graph.length()
                + ByteUtils.SIZE_BYTE
                + ByteUtils.SIZE_INT
                + defaultValueSize;
    }

    public static KeyValue getKey(String graph, String name) {
        return new KeyValue(ByteUtils.concat(graph.getBytes(), GraphConstant.KEY_DELIMITER.getBytes(), new byte[]{MetaType.PROPERTY.getValue()}, GraphConstant.KEY_DELIMITER.getBytes(), name.getBytes()), null);
    }

    @Override
    protected byte[] key() {
        return ByteUtils.concat(graph.getBytes(), GraphConstant.KEY_DELIMITER.getBytes(), new byte[]{type.getValue()}, GraphConstant.KEY_DELIMITER.getBytes(), name.getBytes());
    }

    @Override
    public byte[] getBytes() {
        int pos = super.size();
        byte[] data = super.getBytes();
        pos = ByteUtils.putInt(data, pos, graph.length());
        pos = ByteUtils.putString(data, pos, graph);
        pos = ByteUtils.put(data, pos, dataType.getValue());
        switch (dataType) {
            case LONG:
            case FLOAT:
            case DOUBLE:
            case TIMESTAMP:
                ByteUtils.putInt(data, pos, ByteUtils.SIZE_LONG);
                ByteUtils.putLong(data, pos, (long) defaultValue);
                break;
            case STRING:
                ByteUtils.putInt(data, pos, defaultValue.toString().length());
                ByteUtils.putString(data, pos, (String) defaultValue);
                break;
            case INTEGER:
                ByteUtils.putInt(data, pos, ByteUtils.SIZE_INT);
                ByteUtils.putInt(data, pos, (int) defaultValue);
                break;
            default:
                break;
        }
        return data;
    }

    @Override
    public List<KeyValue> toKVS() {
        List<KeyValue> result = super.toKVS();
        byte[] key = key();
        byte[] data = null;
        int pos = 0;
        switch (dataType) {
            case LONG:
            case FLOAT:
            case DOUBLE:
            case TIMESTAMP:
                data = new byte[ByteUtils.SIZE_LONG];
                ByteUtils.putLong(data, pos, (long) defaultValue);
                break;
            case STRING:
                data = new byte[defaultValue.toString().length()];
                ByteUtils.putString(data, pos, (String) defaultValue);
                break;
            case INTEGER:
                data = new byte[ByteUtils.SIZE_INT];
                ByteUtils.putInt(data, pos, (int) defaultValue);
                break;
            default:
                break;
        }
        KeyValue vkv = new KeyValue(key, "v".getBytes(), data);
        KeyValue tkv = new KeyValue(key, "d".getBytes(), new byte[]{dataType.getValue()});
        result.add(tkv);
        result.add(vkv);
        return result;
    }
}
