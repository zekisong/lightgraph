package com.lightgraph.graph.graph;

import com.lightgraph.graph.writable.Writable;

import java.util.ArrayList;
import java.util.List;

public class EdgeMetaInfo extends Writable {

    private String graph;
    private String name;
    private String subject;
    private String object;
    private List<String> indices = new ArrayList<>();
    private List<PropertyMetaInfo> properties = new ArrayList<>();

    public EdgeMetaInfo() {
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

    public List<String> getIndices() {
        return indices;
    }

    public void setIndices(List<String> indices) {
        this.indices = indices;
    }
}
