package com.lightgraph.graph.meta;

import com.lightgraph.graph.modules.storage.Key;
import java.beans.Transient;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LabelMeta extends ElementMeta {

    protected Long graphId;
    protected List<PropertyMeta> properties;
    protected Map<String, List<String>> indices;
    protected transient volatile Map<String, PropertyMeta> nameMapping = null;
    protected transient volatile Map<Long, PropertyMeta> idMapping = null;

    public Long getGraphId() {
        return graphId;
    }

    public void setGraphId(Long graphId) {
        this.graphId = graphId;
    }

    public LabelMeta() {
    }

    public LabelMeta(String name, MetaType type) {
        super(name, type);
    }

    public List<PropertyMeta> getProperties() {
        return properties;
    }

    public void setProperties(List<PropertyMeta> properties) {
        this.properties = properties;
    }

    @Override
    public void setId(Long id) {
        super.setId(id);
    }

    public Map<String, List<String>> getIndices() {
        return indices;
    }

    public void setIndices(Map<String, List<String>> indices) {
        this.indices = indices;
    }

    @Transient
    public Map<String, PropertyMeta> getNameMapping() {
        if (nameMapping == null) {
            synchronized (LabelMeta.class) {
                Map map = new HashMap<>();
                properties.forEach(p -> {
                    map.put(p.getName(), p);
                });
                nameMapping = map;
            }
        }
        return nameMapping;
    }

    @Transient
    public Map<Long, PropertyMeta> getIdMapping() {
        if (idMapping == null) {
            synchronized (LabelMeta.class) {
                Map map = new HashMap<>();
                properties.forEach(p -> {
                    map.put(p.getId(), p);
                });
                idMapping = map;
            }
        }
        return idMapping;
    }

    @Override
    protected Key key() {
        return makeKey(type, graphId, name);
    }

    public static Key makeKey(MetaType type, Long graphId, String name) {
        return new Key(2, type.getValue(), graphId, name.getBytes());
    }

    public static Key makeLabelScanKey(MetaType type, Long graphId) {
        return new Key(2, type.getValue(), graphId);
    }
}
