package com.lightgraph.graph.meta;

import com.lightgraph.graph.constant.GraphConstant;
import com.lightgraph.graph.modules.storage.KeyValue;
import com.lightgraph.graph.utils.ByteUtils;

import java.util.*;

public class EdgeMeta extends ElementMeta {
    private String graph;
    private Long subject;
    private Long object;
    private Set<Long> properties = new HashSet<>();

    public EdgeMeta(byte[] bytes) {
        super(bytes);
        int pos = super.size();
        int graphSize = ByteUtils.getInt(bytes, pos);
        pos = pos + ByteUtils.SIZE_INT;
        graph = ByteUtils.getString(bytes, pos, graphSize);
        pos = pos + graphSize;
        subject = ByteUtils.getLong(bytes, pos);
        pos = pos + ByteUtils.SIZE_LONG;
        object = ByteUtils.getLong(bytes, pos);
        pos = pos + ByteUtils.SIZE_LONG;
        while (pos < bytes.length) {
            Long pid = ByteUtils.getLong(bytes, pos);
            pos = pos + ByteUtils.SIZE_LONG;
            properties.add(pid);
        }
    }

    public EdgeMeta(List<KeyValue> keyValues) {
        super(keyValues);
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
                case 's':
                    subject = ByteUtils.getLong(value, 0);
                    break;
                case 'o':
                    object = ByteUtils.getLong(value, 0);
                    break;
                case 'p':
                    byte[] pidb = ByteUtils.split(secKey, GraphConstant.KEY_DELIMITER.getBytes()).get(1);
                    long pid = ByteUtils.getLong(pidb, 0);
                    properties.add(pid);
                    break;
            }
        }
    }

    public String getGraph() {
        return graph;
    }

    public void setGraph(String graph) {
        this.graph = graph;
    }

    public EdgeMeta(String name) {
        super(name, MetaType.EDGE);
    }

    public Set<Long> getProperties() {
        return properties;
    }

    public void setProperties(Set<Long> properties) {
        this.properties = properties;
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

    @Override
    public int size() {
        return super.size()
                + ByteUtils.SIZE_INT
                + graph.length()
                + ByteUtils.SIZE_LONG
                + ByteUtils.SIZE_LONG
                + properties.size() * ByteUtils.SIZE_LONG;
    }

    @Override
    public byte[] getBytes() {
        int pos = super.size();
        byte[] data = super.getBytes();
        pos = ByteUtils.putInt(data, pos, graph.length());
        pos = ByteUtils.putString(data, pos, graph);
        pos = ByteUtils.putLong(data, pos, subject);
        pos = ByteUtils.putLong(data, pos, object);
        for (Long pid : properties) {
            pos = ByteUtils.putLong(data, pos, pid);
        }
        return data;
    }

    @Override
    public byte[] key() {
        return ByteUtils.concat(graph.getBytes(), GraphConstant.KEY_DELIMITER.getBytes(), new byte[]{type.getValue()}, GraphConstant.KEY_DELIMITER.getBytes(), name.getBytes());
    }

    @Override
    public List<KeyValue> toKVS() {
        List<KeyValue> result = super.toKVS();
        byte[] key = key();
        byte[] sid = new byte[ByteUtils.SIZE_LONG];
        byte[] oid = new byte[ByteUtils.SIZE_LONG];
        ByteUtils.putLong(sid, 0, subject);
        ByteUtils.putLong(oid, 0, object);
        KeyValue gkv = new KeyValue(key, "g".getBytes(), graph.getBytes());
        KeyValue skv = new KeyValue(key, "s".getBytes(), sid);
        KeyValue okv = new KeyValue(key, "o".getBytes(), oid);
        result.add(gkv);
        result.add(skv);
        result.add(okv);
        for (Long pid : properties) {
            byte[] pidb = new byte[ByteUtils.SIZE_LONG];
            ByteUtils.putLong(pidb, 0, pid);
            KeyValue pkv = new KeyValue(key, ByteUtils.concat(new byte[]{'p'}, GraphConstant.KEY_DELIMITER.getBytes(), pidb), new byte[]{});
            result.add(pkv);
        }
        return result;
    }

    public static KeyValue getKey(String graph, String name) {
        return new KeyValue(ByteUtils.concat(graph.getBytes(), GraphConstant.KEY_DELIMITER.getBytes(), new byte[]{MetaType.EDGE.getValue()}, GraphConstant.KEY_DELIMITER.getBytes(), name.getBytes()), null);
    }
}
