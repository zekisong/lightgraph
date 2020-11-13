package com.lightgraph.graph.graph;

import com.lightgraph.graph.utils.ByteUtils;
import com.lightgraph.graph.writable.Sizeable;
import com.lightgraph.graph.writable.Writable;

import java.util.ArrayList;
import java.util.List;

public class EdgeMetaInfo implements Writable, Sizeable {
    private String graph;
    private String name;
    private String subject;
    private String object;
    private List<PropertyMetaInfo> properties = new ArrayList<>();

    public EdgeMetaInfo() {
    }

    public EdgeMetaInfo(byte[] bytes) {
        int pos = ByteUtils.RESERVED_BYTE_SIZE＿FOR_TX;
        int graphSize = ByteUtils.getInt(bytes, pos);
        pos = pos + ByteUtils.SIZE_INT;
        graph = ByteUtils.getString(bytes, pos, graphSize);
        pos = pos + graphSize;
        int nameSize = ByteUtils.getInt(bytes, pos);
        pos = pos + ByteUtils.SIZE_INT;
        name = ByteUtils.getString(bytes, pos, nameSize);
        pos = pos + nameSize;
        int subjectSize = ByteUtils.getInt(bytes, pos);
        pos = pos + ByteUtils.SIZE_INT;
        subject = ByteUtils.getString(bytes, pos, subjectSize);
        pos = pos + subjectSize;
        int objectSize = ByteUtils.getInt(bytes, pos);
        pos = pos + ByteUtils.SIZE_INT;
        object = ByteUtils.getString(bytes, pos, objectSize);
        pos = pos + objectSize;
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

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getObject() {
        return object;
    }

    public void setObject(String object) {
        this.object = object;
    }

    public List<PropertyMetaInfo> getProperties() {
        return properties;
    }

    public void setProperties(List<PropertyMetaInfo> properties) {
        this.properties = properties;
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
                + subject.length()
                + ByteUtils.SIZE_INT
                + object.length()
                + propertiesSize;
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
        pos = ByteUtils.putInt(data, pos, subject.length());
        pos = ByteUtils.putString(data, pos, subject);
        pos = ByteUtils.putInt(data, pos, object.length());
        pos = ByteUtils.putString(data, pos, object);
        for (PropertyMetaInfo info : properties) {
            pos = ByteUtils.putInt(data, pos, info.size());
            pos = ByteUtils.putBytes(data, pos, info.getBytes());
        }
        return data;
    }
}
