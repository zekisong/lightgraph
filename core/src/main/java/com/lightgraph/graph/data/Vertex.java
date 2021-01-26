package com.lightgraph.graph.data;

import com.lightgraph.graph.meta.MetaType;
import com.lightgraph.graph.meta.PropertyMeta;
import com.lightgraph.graph.meta.VertexMeta;

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

public class Vertex extends GraphElements<VertexMeta> implements Persistable, Indexable {

    private Long id;
    private List<Property> properties = new ArrayList<>();
    private transient Map<String, Property> propertiesMap = new HashMap<>();

    public Vertex() {
    }

    public Vertex(VertexMeta meta) {
        super(meta);
    }

    @Override
    public void setMeta(VertexMeta meta) {
        this.meta = meta;
        properties.forEach(p -> {
            PropertyMeta pm = meta.getIdMapping().get(p.getPid());
            p.setMeta(pm);
        });
    }

    public <T> void addProperty(String k, T value) {
        Property property = new Property(meta.getNameMapping().get(k), value);
        propertiesMap.put(k, property);
        properties.add(property);
    }

    public <T> T getProperty(String key) {
        Object value = propertiesMap.get(key).getValue();
        if (value != null) {
            return (T) value;
        }
        return null;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    @Transient
    public List<Property> getProperties() {
        return properties;
    }

    public void setProperties(List<Property> properties) {
        this.properties = properties;
    }

    public static Key makeKey(Long gid, Long vid, Long id) {
        return new Key(2, gid, vid, id);
    }

    @Override
    public List<KeyValue> toMutations() {
        List<KeyValue> kvs = new ArrayList<>();
        Key key = makeKey(meta.getGraphId(), meta.getId(), id);
        byte[] value = SerdeUtils.getBytes(this);
        KeyValue kv = new KeyValue(key, value);
        kv.setValue(value);
        kvs.add(kv);
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
