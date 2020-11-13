package com.lightgraph.graph.meta;

import com.lightgraph.graph.constant.GraphConstant;
import com.lightgraph.graph.modules.storage.KeyValue;
import com.lightgraph.graph.utils.ByteUtils;

import java.util.*;

public class VertexMeta extends ElementMeta {
    private String graph;
    private String pk;
    private Set<Long> properties = new HashSet<>();

    public VertexMeta(byte[] bytes) {
        super(bytes);
        int pos = super.size();
        int graphSize = ByteUtils.getInt(bytes, pos);
        pos = pos + ByteUtils.SIZE_INT;
        graph = ByteUtils.getString(bytes, pos, graphSize);
        pos = pos + graphSize;
        int keySize = ByteUtils.getInt(bytes, pos);
        pos = pos + ByteUtils.SIZE_INT;
        pk = ByteUtils.getString(bytes, pos, keySize);
        pos = pos + keySize;
        while (pos < bytes.length) {
            long pid = ByteUtils.getLong(bytes, pos);
            pos = pos + ByteUtils.SIZE_LONG;
            properties.add(pid);
        }
    }

    public VertexMeta(List<KeyValue> keyValues) {
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
                case 'k':
                    pk = new String(value);
                    break;
                case 'p':
                    int pos = 0;
                    while (pos < value.length) {
                        long pid = ByteUtils.getLong(value, pos);
                        properties.add(pid);
                        pos = pos + ByteUtils.SIZE_LONG;
                    }
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

    public VertexMeta(String name) {
        super(name, MetaType.VERTEX);
    }

    public Set<Long> getProperties() {
        return properties;
    }

    public void setProperties(Set<Long> properties) {
        this.properties = properties;
    }

    public String getPK() {
        return pk;
    }

    public void setPK(String key) {
        this.pk = key;
    }

    @Override
    public int size() {
        return super.size()
                + ByteUtils.SIZE_INT
                + graph.length()
                + ByteUtils.SIZE_INT
                + pk.length()
                + properties.size() * ByteUtils.SIZE_LONG;
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
        pos = ByteUtils.putInt(data, pos, pk.length());
        pos = ByteUtils.putString(data, pos, pk);
        for (Long pid : properties) {
            pos = ByteUtils.putLong(data, pos, pid);
        }
        return data;
    }

    @Override
    public List<KeyValue> toKVS() {
        List<KeyValue> result = super.toKVS();
        byte[] key = key();
        KeyValue kkv = new KeyValue(key, "k".getBytes(), pk.getBytes());
        byte[] pb = new byte[properties.size() * ByteUtils.SIZE_LONG];
        int pos = 0;
        for (Long pid : properties) {
            ByteUtils.putLong(pb, pos, pid);
            pos = pos + ByteUtils.SIZE_LONG;
        }
        KeyValue pkv = new KeyValue(key, "p".getBytes(), pb);
        result.add(kkv);
        result.add(pkv);
        return result;
    }

    public static KeyValue getKey(String graph, String name) {
        return new KeyValue(ByteUtils.concat(graph.getBytes(), GraphConstant.KEY_DELIMITER.getBytes(), new byte[]{MetaType.VERTEX.getValue()}, GraphConstant.KEY_DELIMITER.getBytes(), name.getBytes()), null);
    }
}
