package com.lightgraph.graph.settings;


public class GraphSetting extends Setting {

    public static final String GRAPH_PARTITION_COUNT = "partition_count";
    public static final String GRAPH_PEPLICATION_COUNT = "replication_count";
    public static final int GRAPH_PARTITION_COUNT_DEFAULT = 10;
    public static final int GRAPH_PEPLICATION_COUNT_DEFAULT = 3;
    private String graphName;

    public GraphSetting() {
    }

    public GraphSetting(String graphName) {
        this.graphName = graphName;
    }

    public String getGraphName() {
        return graphName;
    }

    public void setGraphName(String graphName) {
        this.graphName = graphName;
    }

    @Override
    public String toString() {
        return "{graph:" + graphName + ",config:" + getConfig() + "}";
    }
}
