package raft.constant;

import com.lightgraph.graph.constant.GraphConstant;

public class RaftConstant {

    public static final String RAFT_TIMEWHEEL_INTERVAL = "graph.timewheel.interval";
    public static final long RAFT_TIMEWHEEL_INTERVAL_DEFAULT = 50;

    public static final String RAFT_TIMEWHEEL_SLOT_COUNT = "graph.timewheel.slot.count";
    public static final int RAFT_TIMEWHEEL_SLOT_COUNT_DEFAULT = 5;

    public static final String RAFT_HEARTBEAT_TIMEOUT = "raft.heartbeat.timeout";
    public static final long RAFT_HEARTBEAT_TIMEOUT_DEFAULT = 500;

    public static final String RAFT_ELECTION_TIMEOUT = "raft.election.timeout";
    public static final long RAFT_ELECTION_TIMEOUT_DEFAULT = 5000;

    public static final String RAFT_LOGSEGMENT_SIZE = "raft.logsegment.size";
    public static final long RAFT_LOGSEGMENT_SIZE_DEFAULT = 10 * 1024 * 1024;

    public static final String RAFT_LOG_PATH = "raft.log.path";
    public static final String RAFT_LOG_PATH_DEFAULT = GraphConstant.GRAPH_HOME + "/transaction";

}
