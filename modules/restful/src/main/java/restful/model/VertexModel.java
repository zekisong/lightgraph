package restful.model;

import com.lightgraph.graph.data.Vertex;
import java.util.HashMap;
import java.util.Map;

public class VertexModel {

    private String label;
    private Long id;
    private Map<String, Object> properties = new HashMap();

    public VertexModel(Vertex vertex) {
        this.label = vertex.getMeta().getName();
        this.id = vertex.getId();
        vertex.getProperties().forEach(p -> properties.put(p.getMeta().getName(), p.getValue()));
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }
}
