package com.lightgraph.graph.graph;

import com.lightgraph.graph.writable.Writable;

import java.util.ArrayList;
import java.util.List;

public class VertexMetaInfo extends Writable {

    private String graph;
    private String name;
    private String key;
    private List<String> indices = new ArrayList<>();
    private List<PropertyMetaInfo> properties = new ArrayList<>();

    public VertexMetaInfo() {
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

    public List<String> getIndices() {
        return indices;
    }

    public void setIndices(List<String> indices) {
        this.indices = indices;
    }
}
