package restful.model;

import com.lightgraph.graph.data.Edge;
import java.util.HashMap;
import java.util.Map;

public class EdgeModel {

    private String label;
    private Long subject;
    private Long object;
    private Map<String, Object> properties = new HashMap();

    public EdgeModel() {

    }

    public EdgeModel(Edge edge) {
        this.subject = edge.getSubject();
        this.object = edge.getObject();
        label = edge.getMeta().getName();
        edge.getProperties().values().forEach(p -> properties.put(p.getMeta().getName(), p.getValue()));
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
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

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }
}
