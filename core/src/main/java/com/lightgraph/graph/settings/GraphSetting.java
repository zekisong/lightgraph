package com.lightgraph.graph.settings;


import com.lightgraph.graph.utils.ByteUtils;

public class GraphSetting extends Setting {
    public static final String GRAPH_PARTITION_COUNT = "partition_count";
    public static final String GRAPH_PEPLICATION_COUNT = "replication_count";
    public static final int GRAPH_PARTITION_COUNT_DEFAULT = 10;
    public static final int GRAPH_PEPLICATION_COUNT_DEFAULT = 3;
    private String graphName;

    public GraphSetting(byte[] bytes) {
        super(bytes);
        int graphNameOffset = super.size();
        this.graphName = ByteUtils.getString(bytes, graphNameOffset, bytes.length - graphNameOffset);
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
    public int size() {
        return super.size() + graphName.length();
    }

    @Override
    public byte[] getBytes() {
        byte[] data = super.getBytes();
        ByteUtils.putString(data, super.size(), graphName);
        return data;
    }

    @Override
    public String toString() {
        return "{graph:" + graphName + ",config:" + getConfig() + "}";
    }
}
