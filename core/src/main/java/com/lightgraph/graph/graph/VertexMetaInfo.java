package com.lightgraph.graph.graph;

import com.lightgraph.graph.utils.ByteUtils;
import com.lightgraph.graph.writable.Sizeable;
import com.lightgraph.graph.writable.Writable;

import java.util.ArrayList;
import java.util.List;

public class VertexMetaInfo implements Writable, Sizeable {
    private String graph;
    private String name;
    private String key;
    private List<PropertyMetaInfo> properties = new ArrayList<>();

    public VertexMetaInfo() {
    }

    public VertexMetaInfo(byte[] bytes) {
        int pos = ByteUtils.RESERVED_BYTE_SIZE＿FOR_TX;
        int graphSize = ByteUtils.getInt(bytes, pos);
        pos = pos + ByteUtils.SIZE_INT;
        graph = ByteUtils.getString(bytes, pos, graphSize);
        pos = pos + graphSize;
        int nameSize = ByteUtils.getInt(bytes, pos);
        pos = pos + ByteUtils.SIZE_INT;
        name = ByteUtils.getString(bytes, pos, nameSize);
        pos = pos + nameSize;
        int keySize = ByteUtils.getInt(bytes, pos);
        pos = pos + ByteUtils.SIZE_INT;
        key = ByteUtils.getString(bytes, pos, keySize);
        pos = pos + keySize;
        while (pos < bytes.length) {
            int psize = ByteUtils.getInt(bytes, pos);
            pos = pos + ByteUtils.SIZE_INT;
            byte[] pb = ByteUtils.getBytes(bytes, pos, psize);
            pos = pos + psize;
            properties.add(new PropertyMetaInfo(pb));
        }
    }

    public String getGraph() {
        return graph;
    }

    public void setGraph(String graph) {
        this.graph = graph;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public List<PropertyMetaInfo> getProperties() {
        return properties;
    }

    public void setProperties(List<PropertyMetaInfo> properties) {
        this.properties = properties;
    }

    @Override
    public byte[] getBytes() {
        int size = size();
        byte[] data = new byte[size];
        int pos = ByteUtils.RESERVED_BYTE_SIZE＿FOR_TX;
        pos = ByteUtils.putInt(data, pos, graph.length());
        pos = ByteUtils.putString(data, pos, graph);
        pos = ByteUtils.putInt(data, pos, name.length());
        pos = ByteUtils.putString(data, pos, name);
        pos = ByteUtils.putInt(data, pos, key.length());
        pos = ByteUtils.putString(data, pos, key);
        for (PropertyMetaInfo info : properties) {
            pos = ByteUtils.putInt(data, pos, info.size());
            pos = ByteUtils.putBytes(data, pos, info.getBytes());
        }
        return data;
    }

    @Override
    public int size() {
        int propertiesSize = 0;
        for (PropertyMetaInfo info : properties) {
            propertiesSize = propertiesSize + +ByteUtils.SIZE_INT + info.size();
        }
        return ByteUtils.RESERVED_BYTE_SIZE＿FOR_TX
                + ByteUtils.SIZE_INT
                + graph.length()
                + ByteUtils.SIZE_INT
                + name.length()
                + ByteUtils.SIZE_INT
                + key.length()
                + propertiesSize;
    }
}
