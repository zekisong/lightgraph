package restful.model;

import java.io.Serializable;

public class GraphMetaRequestModel implements Serializable {
    private int partition;
    private int replicas;

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public int getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }
}
