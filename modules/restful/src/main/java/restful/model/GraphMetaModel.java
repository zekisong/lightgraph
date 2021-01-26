package restful.model;

import com.lightgraph.graph.meta.cluster.GraphMeta;

import java.text.SimpleDateFormat;
import java.util.Map;

public class GraphMetaModel {

    private static ThreadLocal<SimpleDateFormat> format = new ThreadLocal<>();
    private String graph;
    private Long id;
    private Map<String, String> config;
    private String createTime;

    public GraphMetaModel(GraphMeta meta) {
        this.graph = meta.getName();
        this.config = meta.getSetting().getConfig();
        this.id = meta.getId();
        SimpleDateFormat sdf = format.get();
        if (sdf == null) {
            sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
        this.createTime = sdf.format(meta.getCreateTime());
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getGraph() {
        return graph;
    }

    public void setGraph(String graph) {
        this.graph = graph;
    }

    public Map<String, String> getConfig() {
        return config;
    }

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }
}
