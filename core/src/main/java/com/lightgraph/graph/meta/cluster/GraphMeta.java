package com.lightgraph.graph.meta.cluster;

import com.lightgraph.graph.meta.ElementMeta;
import com.lightgraph.graph.meta.MetaType;
import com.lightgraph.graph.modules.storage.Key;
import com.lightgraph.graph.settings.GraphSetting;

public class GraphMeta extends ElementMeta {

    private GraphSetting setting;
    private long version;

    public GraphMeta() {

    }

    public GraphMeta(String name) {
        super(name, MetaType.GRAPH);
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public GraphSetting getSetting() {
        return setting;
    }

    public void setSetting(GraphSetting setting) {
        this.setting = setting;
    }

    public static Key makeKey(String name) {
        return new Key(1, MetaType.GRAPH.getValue(), name);
    }

    public static Key makeGraphScanKey() {
        return new Key(1, MetaType.GRAPH.getValue());
    }

    @Override
    public String toString() {
        return super.toString() + ",graphSettting:" + setting.toString();
    }

    @Override
    protected Key key() {
        return makeKey(name);
    }

}
