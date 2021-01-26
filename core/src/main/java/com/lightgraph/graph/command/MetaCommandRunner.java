package com.lightgraph.graph.command;

import com.lightgraph.graph.cluster.Replication;
import com.lightgraph.graph.cluster.node.Node;
import com.lightgraph.graph.cluster.manager.NodeManager;
import com.lightgraph.graph.graph.EdgeMetaInfo;
import com.lightgraph.graph.graph.VertexMetaInfo;
import com.lightgraph.graph.modules.storage.BackendStorageHandler;
import com.lightgraph.graph.settings.GraphSetting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
                Node node = Node.getInstance(r, 1, r.length - 1, Node.class);
                info = info + node.toString();
                manager.setLeaderInner(node);
                break;
            case ADD_NODE:
                node = Node.getInstance(r, 1, r.length - 1, Node.class);
                info = info + node.toString();
                manager.joinToClusterInner(node);
                break;
            case ADD_REPLICATION:
                Replication replication = Replication.getInstance(r, 1, r.length - 1, Replication.class);
                info = info + replication.getDescription();
                manager.addReplicationInner(replication);
                break;
            case UPDATE_REPLICATION:
                replication = Replication.getInstance(r, 1, r.length - 1, Replication.class);
                info = info + replication.getDescription();
                manager.updateReplicationInner(replication);
                break;
            case CREATE_GRAPH:
                GraphSetting setting = GraphSetting.getInstance(r, 1, r.length - 1, GraphSetting.class);
                info = info + setting.toString();
                manager.createGraphInner(setting);
                break;
            case ADD_VERTEX_META:
                VertexMetaInfo vmi = VertexMetaInfo.getInstance(r, 1, r.length - 1, VertexMetaInfo.class);
                manager.addVertexMetaInner(vmi);
                break;
            case ADD_EDGE_META:
                EdgeMetaInfo emi = EdgeMetaInfo.getInstance(r, 1, r.length - 1, EdgeMetaInfo.class);
                manager.addEdgeMetaInner(emi);
                break;
        }
        LOG.info(info);
    }

}