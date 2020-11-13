package com.lightgraph.graph.data;

import com.lightgraph.graph.meta.VertexMeta;

import java.util.Map;

public class Vertex extends GraphElements {
    private Object id;
    Map<String, Property> properties;

    public Vertex(VertexMeta meta) {
        super(meta);
    }

    public void addProperty(String k, Property value) {
        properties.put(k, value);
    }

    public <T> T getProperty(String key) {
        Object value = properties.get(key).getValue();
        if (value != null)
            return (T) value;
        return null;
    }

    public <T> T getId() {
        return (T) id;
    }

    public <T> void setId(T id) {
        this.id = id;
    }

    public Map<String, Property> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Property> properties) {
        this.properties = properties;
    }
}
