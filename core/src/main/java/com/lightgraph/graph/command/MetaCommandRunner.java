package com.lightgraph.graph.command;

import com.lightgraph.graph.cluster.Replication;
import com.lightgraph.graph.cluster.node.Node;
import com.lightgraph.graph.cluster.manager.NodeManager;
import com.lightgraph.graph.graph.EdgeMetaInfo;
import com.lightgraph.graph.graph.VertexMetaInfo;
import com.lightgraph.graph.meta.PropertyMeta;
import com.lightgraph.graph.meta.VertexMeta;
import com.lightgraph.graph.modules.storage.BackendStorageHandler;
import com.lightgraph.graph.modules.storage.KeyValue;
import com.lightgraph.graph.settings.GraphSetting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

public class MetaCommandRunner extends CommandRunner {

    private static final Log LOG = LogFactory.getLog(MetaCommandRunner.class);
    private NodeManager manager;
    private BackendStorageHandler metaStorage;

    public MetaCommandRunner(NodeManager manager, CommandObserver observer) {
        super(manager.getConsensusIO(), observer);
        this.manager = manager;
    }

    public void setMetaStorage(BackendStorageHandler metaStorage) {
        this.metaStorage = metaStorage;
    }

    @Override
    public void runCommand(byte[] r) {
        MasterOperation op = MasterOperation.valueOf(r[0]);
        String info = "run meta command=>" + op + "\t";
        switch (op) {
            case SET_MASTER_LEADER:
                Node node = new Node(r);
                info = info + node.toString();
                manager.setLeaderInner(node);
                break;
            case ADD_NODE:
                node = new Node(r);
                info = info + node.toString();
                manager.joinToClusterInner(node);
                break;
            case ADD_REPLICATION:
                Replication replication = new Replication(r);
                info = info + replication.getDescription();
                manager.addReplicationInner(replication);
                break;
            case UPDATE_REPLICATION:
                replication = new Replication(r);
                info = info + replication.getDescription();
                manager.updateReplicationInner(replication);
                break;
            case CREATE_GRAPH:
                GraphSetting setting = new GraphSetting(r);
                info = info + setting.toString();
                manager.createGraphInner(setting);
                break;
            case ADD_VERTEX_META:
                VertexMetaInfo vmi = new VertexMetaInfo(r);
                manager.addVertexMetaInner(vmi);
                break;
            case ADD_EDGE_META:
                EdgeMetaInfo emi = new EdgeMetaInfo(r);
                manager.addEdgeMetaInner(emi);
                break;
        }
        LOG.info(info);
    }

}