package com.lightgraph.graph.data;

import com.lightgraph.graph.meta.EdgeMeta;

import com.lightgraph.graph.meta.MetaType;
import com.lightgraph.graph.meta.PropertyMeta;
import com.lightgraph.graph.modules.storage.Key;
import com.lightgraph.graph.modules.storage.KeyValue;
import com.lightgraph.graph.utils.ByteUtils;
import com.lightgraph.graph.utils.SerdeUtils;
import com.lightgraph.graph.writable.Indexable;
import com.lightgraph.graph.writable.Persistable;
import java.beans.Transient;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Edge extends GraphElements<EdgeMeta> implements Persistable, Indexable {

    private Long subject;
    private Long object;
    private transient Map<String, Property> properties = new HashMap<>();

    public Edge() {
    }

    public Edge(EdgeMeta meta) {
        super(meta);
    }

    public void setMeta(EdgeMeta meta) {
        this.meta = meta;
        properties.values().forEach(p -> {
            PropertyMeta pm = meta.getIdMapping().get(p.getPid());
            p.setMeta(pm);
        });
    }

    public <T> void addProperty(String k, T value) {
        properties.put(k, new Property(meta.getNameMapping().get(k), value));
    }

    public <T> T getProperty(String key) {
        Object value = properties.get(key).getValue();
        return value == null ? null : (T) value;
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

    @Transient
    public Map<String, Property> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Property> properties) {
        this.properties = properties;
    }

    public static Key makeKey(Long gid, Long subject, Long eid, Long object) {
        return new Key(1, gid, subject, eid, object);
    }

    public static Key makeSubjectScanKey(Long gid, Long subject) {
        return new Key(1, gid, subject);
    }

    public static Key makeLabelScanKey(Long gid, Long subject, Long eid) {
        return new Key(1, gid, subject, eid);
    }

    public static Long getEdgeMetaIdFromKeyValue(KeyValue kv) {
        return SerdeUtils.getObject(kv.getKey().getIndex(2), Long.class);
    }

    @Override
    public List<KeyValue> toMutations() {
        List<KeyValue> kvs = new ArrayList<>();
        Key key = makeKey(meta.getGraphId(), subject, meta.getId(), object);
        byte[] value = SerdeUtils.getBytes(this);
        KeyValue kv = new KeyValue(key, value);
        kv.setValue(value);
        kvs.add(kv);
        for (Key indexKey : indexKeys()) {
            kvs.add(new KeyValue(indexKey, kv.getKey().bytes()));
        }
        return kvs;
    }

    @Override
    public List<Key> indexKeys() {
        Map<String, List<String>> indices = meta.getIndices();
        if (indices != null && indices.size() > 0) {
            List<Key> indexKeys = new ArrayList<>();
            for (String indexName : indices.keySet()) {
                byte[] indexkey = new byte[]{MetaType.INDEX.getValue()};
                List<String> indexFields = indices.get(indexName);
                for (String indexField : indexFields) {
                    Object value = getProperty(indexField);
                    indexkey = ByteUtils.concat(indexkey, SerdeUtils.getBytes(value));
                }
                indexKeys.add(new Key(0, indexkey));
            }
            return indexKeys;
        } else {
            return Collections.emptyList();
        }
    }
}
