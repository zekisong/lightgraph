package com.lightgraph.graph.data;

import com.lightgraph.graph.exception.GraphException;
import com.lightgraph.graph.meta.EdgeMeta;
import com.lightgraph.graph.meta.ElementMeta;
import com.lightgraph.graph.meta.VertexMeta;

import java.util.HashMap;
import java.util.Map;

public class ElementBuilder {
    private ElementMeta meta;
    private Object id;
    private Map<String, Property> properties = new HashMap();

    public static ElementBuilder newBuilder() {
        return new ElementBuilder();
    }

    public ElementBuilder withMeta(EdgeMeta meta) {
        this.meta = meta;
        return this;
    }

    public <T> ElementBuilder withID(T id) {
        this.id = id;
        return this;
    }

    //TODO p == null print warn information or throw exception ?
    public ElementBuilder withProperty(Property p) {
        if (p != null)
            properties.put(p.getMeta().getName(), p);
        return this;
    }

    public <T extends GraphElements> T build() {
        if (meta instanceof VertexMeta) {
            Vertex vertex = new Vertex((VertexMeta) meta);
            vertex.setId(id);
            vertex.setProperties(properties);
            return (T) vertex;
        } else if (meta instanceof EdgeMeta) {
            Edge edge = new Edge((EdgeMeta) meta);
            edge.setId(id);
            edge.setProperties(properties);
            return (T) edge;
        } else {
            throw new GraphException(String.format("not support this meta:%s", meta));
        }
    }
}
