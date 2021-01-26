package restful.model;

import com.lightgraph.graph.cluster.Partition;
import com.lightgraph.graph.cluster.Replication;
import com.lightgraph.graph.modules.consensus.ConsensusInstanceState;
import com.lightgraph.graph.server.Server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RouttingModel {

    private Map<String, List<PartitionModel>> routting = new HashMap<>();

    public Map<String, List<PartitionModel>> getRoutting() {
        return routting;
    }

    public void setRoutting(Map<String, List<PartitionModel>> routting) {
        this.routting = routting;
    }

    public RouttingModel() {
        Map<String, Map<Integer, Partition>> clusterRoutting = Server.getInstance().getManager().getRoutting();
        for (String graph : clusterRoutting.keySet()) {
            Map<Integer, Partition> partitions = clusterRoutting.get(graph);
            for (Integer partitionIndex : partitions.keySet()) {
                Partition partition = partitions.get(partitionIndex);
                List<ReplicationModel> replicationModels = new ArrayList<>();
                for (Replication replication : partition.getReplications().values()) {
                    Integer replicationIndex = replication.getReplicationIndex();
                    String location = replication.getLocation().getName();
                    ConsensusInstanceState state = replication.getState();
                    ReplicationModel rm = new ReplicationModel(replicationIndex, location, state);
                    replicationModels.add(rm);
                }
                PartitionModel partitionModel = new PartitionModel(partitionIndex, replicationModels);
                if (routting.containsKey(graph)) {
                    routting.get(graph).add(partitionModel);
                } else {
                    List<PartitionModel> partitionModels = new ArrayList<>();
                    partitionModels.add(partitionModel);
                    routting.put(graph, partitionModels);
                }
            }
        }
    }

    class PartitionModel {

        private Integer PID;
        private List<ReplicationModel> replications = new ArrayList<>();

        public PartitionModel(Integer PID, List<ReplicationModel> replications) {
            this.PID = PID;
            this.replications = replications;
        }

        public Integer getPID() {
            return PID;
        }

        public void setPID(Integer PID) {
            this.PID = PID;
        }

        public List<ReplicationModel> getReplications() {
            return replications;
        }

        public void setReplications(List<ReplicationModel> replications) {
            this.replications = replications;
        }
    }

    class ReplicationModel {

        private Integer RID;
        private String location;
        private ConsensusInstanceState state;

        public ReplicationModel(Integer RID, String location, ConsensusInstanceState state) {
            this.RID = RID;
            this.location = location;
            this.state = state;
        }

        public ConsensusInstanceState getState() {
            return state;
        }

        public void setState(ConsensusInstanceState state) {
            this.state = state;
        }

        public Integer getRID() {
            return RID;
        }

        public void setRID(Integer RID) {
            this.RID = RID;
        }

        public String getLocation() {
            return location;
        }

        public void setLocation(String location) {
            this.location = location;
        }
    }
}
