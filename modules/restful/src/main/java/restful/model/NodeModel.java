package restful.model;


import com.lightgraph.graph.cluster.node.NodeState;
import com.lightgraph.graph.cluster.node.NodeType;

import java.io.Serializable;

public class NodeModel implements Serializable {
    private String name;
    private NodeType type;
    private NodeState state;

    public NodeModel() {
    }

    public NodeModel(String name, NodeType type, NodeState state) {
        this.name = name;
        this.type = type;
        this.state = state;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public NodeType getType() {
        return type;
    }

    public void setType(NodeType type) {
        this.type = type;
    }

    public NodeState getState() {
        return state;
    }

    public void setState(NodeState state) {
        this.state = state;
    }
}