package raft;

import com.lightgraph.graph.config.Configurable;
import com.lightgraph.graph.constant.GraphConstant;
import com.lightgraph.graph.modules.consensus.*;
import com.lightgraph.graph.modules.NetworkModule;
import com.lightgraph.graph.cluster.node.Node;
import com.lightgraph.raft.proto.RaftServiceGrpc;
import io.grpc.*;
import com.lightgraph.graph.timewheel.TimeWheel;
import raft.constant.RaftConstant;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConsensusModuleImpl implements ConsensusModule {

    private TimeWheel timeWheel;
    private Map<ConsensusInstance, RaftQuorum> raftInstanceMap = new ConcurrentHashMap();
    private Map<Node, RaftServiceGrpc.RaftServiceStub> peerMap = new ConcurrentHashMap();
    private Map<Node, RaftServiceGrpc.RaftServiceBlockingStub> syncPeerMap = new ConcurrentHashMap();
    private String logPath;
    private long heartBeatTimeout;
    private long electionTimeout;
    private long segmentSize;

    @Override
    public void setup(Configurable config) {
        long interval = config.get(RaftConstant.RAFT_TIMEWHEEL_INTERVAL, RaftConstant.RAFT_TIMEWHEEL_INTERVAL_DEFAULT);
        int slotCount = config
                .get(RaftConstant.RAFT_TIMEWHEEL_SLOT_COUNT, RaftConstant.RAFT_TIMEWHEEL_SLOT_COUNT_DEFAULT);
        heartBeatTimeout = config.get(RaftConstant.RAFT_HEARTBEAT_TIMEOUT, RaftConstant.RAFT_HEARTBEAT_TIMEOUT_DEFAULT);
        electionTimeout = config.get(RaftConstant.RAFT_ELECTION_TIMEOUT, RaftConstant.RAFT_ELECTION_TIMEOUT_DEFAULT);
        logPath = config.get(RaftConstant.RAFT_LOG_PATH, RaftConstant.RAFT_LOG_PATH_DEFAULT);
        this.timeWheel = new TimeWheel(interval, slotCount);
        segmentSize = config.get(RaftConstant.RAFT_LOGSEGMENT_SIZE, RaftConstant.RAFT_LOGSEGMENT_SIZE_DEFAULT);
        File logDir = new File(logPath);
        if (!logDir.exists()) {
            logDir.mkdir();
        }
    }

    @Override
    public void bindNetwork(NetworkModule networkModule) {
        ServerBuilder builder = networkModule.getBuilder();
        builder.addService(new RaftServiceProxy(this));
    }

    @Override
    public ConsensusHandler loadInstance(ConsensusInstance instance, boolean clean, LeaderChangeListener... listeners) {
        RaftQuorum quorum = loadInstance(instance, clean);
        for (LeaderChangeListener listener : listeners) {
            listener.bindInstance(instance);
            quorum.addLeaderChangeListener(listener);
        }
        raftInstanceMap.put(instance, quorum);
        quorum.install();
        return quorum;
    }

    public RaftQuorum loadInstance(ConsensusInstance instance, boolean clean) {
        RaftQuorum quorum = new RaftQuorum(logPath + GraphConstant.DIR_SEPARATOR + instance.getInstanceName(), this,
                instance, clean);
        return quorum;
    }

    @Override
    public void unLoadInstance(ConsensusGroup instance) {
        if (raftInstanceMap.containsKey(instance.getGroupName())) {
            RaftQuorum node = raftInstanceMap.get(instance);
            if (node != null) {
                node.uninstall();
            }
            raftInstanceMap.remove(instance);
        }
    }

    @Override
    public ConsensusInstance getGroupLeader(ConsensusInstance instance) {
        ConsensusInstance leader = raftInstanceMap.get(instance).getLeader();
        return leader;
    }

    @Override
    public ConsensusHandler getConsensusHandler() {
        return null;
    }

    @Override
    public boolean start() {
        return true;
    }

    @Override
    public boolean shutdown() {
        return true;
    }

    @Override
    public String moduleInfo() {
        return "raft impl of consensus module, clazz:" + this.getClass().getName();
    }

    public void addPeer(Node node) {
        if (!peerMap.containsKey(node)) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress(node.getAddr(), node.getPort())
                    .usePlaintext(true)
                    .build();
            RaftServiceGrpc.RaftServiceStub raftStub = RaftServiceGrpc.newStub(channel);
            peerMap.put(node, raftStub);
        }
    }

    public RaftServiceGrpc.RaftServiceStub getPeer(Node node) {
        RaftServiceGrpc.RaftServiceStub target = peerMap.get(node);
        if (target == null) {
            synchronized (peerMap) {
                if (target == null) {
                    addPeer(node);
                    target = peerMap.get(node);
                }
            }
        }
        return target;
    }

    public void addSyncPeer(Node node) {
        if (!syncPeerMap.containsKey(node)) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress(node.getAddr(), node.getPort())
                    .usePlaintext(true)
                    .build();
            RaftServiceGrpc.RaftServiceBlockingStub raftStub = RaftServiceGrpc.newBlockingStub(channel);
            syncPeerMap.put(node, raftStub);
        }
    }

    public RaftServiceGrpc.RaftServiceBlockingStub getSyncPeer(Node node) {
        RaftServiceGrpc.RaftServiceBlockingStub target = syncPeerMap.get(node);
        if (target == null) {
            synchronized (syncPeerMap) {
                if (target == null) {
                    addSyncPeer(node);
                    target = syncPeerMap.get(node);
                }
            }
        }
        return target;
    }

    public RaftQuorum getRaftQuorum(ConsensusInstance instance) {
        RaftQuorum target = raftInstanceMap.get(instance);
        return target;
    }

    public TimeWheel getTimeWheel() {
        return timeWheel;
    }

    public long getHeartBeatTimeout() {
        return heartBeatTimeout;
    }

    public long getElectionTimeout() {
        return electionTimeout;
    }

    public long getSegmentSize() {
        return segmentSize;
    }
}










