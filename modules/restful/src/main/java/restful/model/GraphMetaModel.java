package restful.model;

import com.lightgraph.graph.meta.cluster.GraphMeta;

import java.text.SimpleDateFormat;
import java.util.Map;

public class GraphMetaModel {
    private static ThreadLocal<SimpleDateFormat> format = new ThreadLocal<>();
    private String graph;
    private Map<String, String> config;
    private String createTime;

    public GraphMetaModel(GraphMeta meta) {
        this.graph = meta.getName();
        this.config = meta.getSetting().getConfig();
        SimpleDateFormat sdf = format.get();
        if (sdf == null) {
            sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
        this.createTime = sdf.format(meta.getCreateTime());
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
