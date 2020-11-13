package restful.model;

import com.lightgraph.graph.server.Server;
import com.lightgraph.graph.cluster.node.Node;
import com.lightgraph.graph.cluster.manager.NodeManager;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

@XmlRootElement
public class ClusterModel {
    private String leader = "NOT FOUND";
    private long version = 0;
    private List<NodeModel> nodes = new ArrayList<>();

    public ClusterModel() {
        NodeManager manager = Server.getInstance().getManager();
        for (Node node : manager.getNodes().values()) {
            NodeModel model = new NodeModel(node.getName(), node.getType(), node.getState());
            this.nodes.add(model);
        }
        version = manager.getNodeVersion();
        Node masterLeader = manager.getLeader();
        if (masterLeader != null)
            this.leader = masterLeader.getName();
        else
            this.leader = "NOT FOUND";
    }

    public String getLeader() {
        return leader;
    }

    public void setLeader(String leader) {
        this.leader = leader;
    }

    public List<NodeModel> getNodes() {
        return nodes;
    }

    public void setNodes(List<NodeModel> nodes) {
        this.nodes = nodes;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }
}
