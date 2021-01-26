package raft;

import com.google.protobuf.ByteString;
import com.lightgraph.graph.modules.consensus.*;
import com.lightgraph.graph.cluster.node.Node;
import com.lightgraph.raft.proto.*;
import io.grpc.stub.StreamObserver;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import raft.log.LogSystem;
import raft.stm.RaftEvent;
import raft.stm.RaftState;
import raft.stm.RaftStateMachine;
import raft.stm.Transition;
import com.lightgraph.graph.timewheel.Task;
import com.lightgraph.graph.timewheel.TaskNode;
import com.lightgraph.graph.timewheel.TimeWheel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * raft protocol impl
 */
public class RaftQuorum extends RaftServiceGrpc.RaftServiceImplBase implements ConsensusHandler {

    private static final Log LOG = LogFactory.getLog(RaftQuorum.class);
    private LogSystem logSystem;
    private ConsensusInstance instance;
    private volatile ConsensusInstance leader = null;
    private volatile ConsensusInstance voteFor = null;
    private RaftStateMachine stateMachine;
    private ConsensusModuleImpl module;
    private TimeWheel timeWheel;
    private TaskNode heartBeatCheckTask;
    private TaskNode electionTimeOutCheckTask;
    private TaskNode heartBeatBoardCastTask;
    private TaskNode raftClusterMetricsTask;
    private volatile long term;
    private AtomicInteger votes = new AtomicInteger(0);
    private List<LeaderChangeListener> leaderChangeListeners = new ArrayList<>();
    private long heartBeatTimeout;
    private long electionTimeout;

    public RaftQuorum(String logPath, ConsensusModuleImpl module, ConsensusInstance instance, boolean clean) {
        this.timeWheel = module.getTimeWheel();
        this.heartBeatTimeout = module.getHeartBeatTimeout();
        this.electionTimeout = module.getElectionTimeout();
        this.module = module;
        this.instance = instance;
        logSystem = new LogSystem(this, logPath, module.getSegmentSize(), timeWheel, clean);
        this.term = logSystem.getPreTerm();
        logSystem.bindInstance(instance);
        leaderChangeListeners.add(logSystem);
    }

    @Override
    public void addLeaderChangeListener(LeaderChangeListener leaderChangeListener) {
        this.leaderChangeListeners.add(leaderChangeListener);
    }

    @Override
    public boolean isLeader() {
        return this.instance.equals(leader);
    }

    //leader action
    @Override
    public WriteFuture write(byte[] record) {
        if (!isLeader()) {
            return null;
        }
        try {
            return logSystem.append(term, record);
        } catch (Exception e) {
            LOG.error("write failed!", e);
            return null;
        }
    }

    @Override
    public byte[] read() {
        return logSystem.getUnapplyed();
    }

    //follower action
    @Override
    public void appendLog(com.lightgraph.raft.proto.AppendLogEditRequest request,
            io.grpc.stub.StreamObserver<com.lightgraph.raft.proto.AppendLogEditResponse> responseObserver) {
        synchronized (stateMachine) {
            if (isLeader() || stateMachine.getCurrentState() != RaftState.FOLLOWER) {
                responseObserver.onCompleted();
                return;
            }
        }
        AppendLogEditResponse.Builder builder = AppendLogEditResponse.newBuilder();
        long term = request.getTerm();
        long preTerm = request.getPreTerm();
        byte[] data = request.getRecord().toByteArray();
        long index = request.getIndex();
        boolean ret = logSystem.append(preTerm, term, index, data);
        builder.setIsok(ret);
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    public void install() {
//        electionTimeOutCheckTask = timeWheel.addTask(new ElectionTimeOutCheckTask(), false);
        heartBeatCheckTask = timeWheel.addTask(new HeartBeatCheckTask(), false);
        raftClusterMetricsTask = timeWheel.addTask(new RaftClusterMetricsTask(), false);
        initRaftStateMachine(RaftState.FOLLOWER);
    }

    public void uninstall() {
        timeWheel.removeTask(heartBeatCheckTask);
        timeWheel.removeTask(electionTimeOutCheckTask);
    }

    public void reset() {
        this.voteFor = null;
        this.votes.set(0);
    }

    public void setLeader(ConsensusInstance leader) {
        this.leader = leader;
        try {
            leaderChangeListeners.forEach(l -> l.onLeaderChangeEvent(term, this.leader));
        } catch (Exception e) {
            LOG.error("set leader failed!", e);
        }
    }

    public void initRaftStateMachine(RaftState initState) {
        stateMachine = new RaftStateMachine(initState);
        stateMachine
                .addTransition(RaftState.FOLLOWER, RaftEvent.HEARTBEAT_TIMEOUT, RaftState.CANDIDATE, new Transition() {
                    @Override
                    public boolean doTransition() {
                        setLeader(null);
                        if (electionTimeOutCheckTask == null) {
                            electionTimeOutCheckTask = timeWheel.addTask(new ElectionTimeOutCheckTask(), false);
                        } else {
                            timeWheel.addTask(electionTimeOutCheckTask);
                        }
                        resetTimeOut();
                        LOG.info(instance.getDescription() + "\tFOLLOWER -> CANDIDATE (WITH HEARTBEAT_TIMEOUT) TERM "
                                + term);
                        return true;
                    }
                })
                .addTransition(RaftState.CANDIDATE, RaftEvent.CANDIDATE_ELECTION_TIMEOUT, RaftState.CANDIDATE,
                        new Transition() {
                            @Override
                            public boolean doTransition() {
                                setLeader(null);
                                resetTimeOut();
                                sendVoteRequest();
                                if (stateMachine.getCurrentState() != RaftState.CANDIDATE) {
                                    return false;
                                } else {
                                    LOG.info(instance.getDescription()
                                            + "\tCANDIDATE -> CANDIDATE (WITH CANDIDATE_ELECTION_TIMEOUT) TERM "
                                            + term);
                                    return true;
                                }
                            }
                        })
                .addTransition(RaftState.CANDIDATE, RaftEvent.RECEIVE_VOTES_FROM_MAJORITY_SERVERS, RaftState.LEADER,
                        new Transition() {
                            @Override
                            public boolean doTransition() {
                                timeWheel.removeTask(heartBeatCheckTask);
                                timeWheel.removeTask(electionTimeOutCheckTask);
                                if (heartBeatBoardCastTask == null) {
                                    heartBeatBoardCastTask = timeWheel.addTask(new HeartBeatBoardCastTask(), false);
                                } else {
                                    timeWheel.addTask(heartBeatBoardCastTask);
                                }
                                LOG.info(instance.getDescription()
                                        + "\tCANDIDATE -> LEADER (WITH RECEIVE_VOTES_FROM_MAJORITY_SERVERS) TERM "
                                        + term);
                                return true;
                            }
                        })
                .addTransition(RaftState.CANDIDATE, RaftEvent.DISCOVERY_CURRENT_LEADER, RaftState.FOLLOWER,
                        new Transition() {
                            @Override
                            public boolean doTransition() {
                                resetTimeOut();
                                reset();
                                LOG.info(instance.getDescription()
                                        + "\tCANDIDATE -> FOLLOWER (WITH DISCOVERY_CURRENT_LEADER) TERM " + term);
                                return true;
                            }
                        })
                .addTransition(RaftState.CANDIDATE, RaftEvent.DISCOVERY_HIGH_TERM, RaftState.FOLLOWER,
                        new Transition() {
                            @Override
                            public boolean doTransition() {
                                resetTimeOut();
                                reset();
                                LOG.info(instance.getDescription()
                                        + "\tCANDIDATE -> FOLLOWER (WITH DISCOVERY_HIGH_TERM) TERM " + term);
                                return true;
                            }
                        })
                .addTransition(RaftState.LEADER, RaftEvent.DISCOVERY_HIGH_TERM, RaftState.FOLLOWER, new Transition() {
                    @Override
                    public boolean doTransition() {
                        timeWheel.removeTask(heartBeatBoardCastTask);
                        timeWheel.addTask(heartBeatCheckTask);
                        timeWheel.addTask(electionTimeOutCheckTask);
                        reset();
                        resetTimeOut();
                        LOG.info(instance.getDescription() + "\tLEADER -> FOLLOWER (WITH DISCOVERY_HIGH_TERM) TERM "
                                + term);
                        return true;
                    }
                });
    }

    @Override
    public void vote(com.lightgraph.raft.proto.VoteRequest request,
            io.grpc.stub.StreamObserver<com.lightgraph.raft.proto.VoteResponse> responseObserver) {
        VoteResponse.Builder responseBuilder = VoteResponse.newBuilder();
        ConsensusInstance candidate = ConsensusInstance
                .getInstance(request.getCandidate().toByteArray(), ConsensusInstance.class);
        long requestTerm = request.getTerm();
        long requestIndex = request.getIndex();
        long currentIndex = logSystem.getLatestEditIndex();
        synchronized (stateMachine) {
            try {
                if (requestTerm > this.term && requestIndex >= currentIndex) {
                    stateMachine.onEvent(RaftEvent.DISCOVERY_HIGH_TERM);
                    this.term = requestTerm;
                    this.voteFor = candidate;
                    resetTimeOut();
                    responseBuilder.setAccept(true);
                } else if (requestTerm > this.term && requestIndex < currentIndex
                        && stateMachine.getCurrentState() == RaftState.LEADER) {
                    this.term = requestTerm + 2;
                    responseBuilder.setAccept(false);
                } else {
                    responseBuilder.setAccept(false);
                }
                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();
            } catch (Throwable t) {
                LOG.error("handle vote failed!", t);
            }
        }
    }

    @Override
    public void keepHeartBeat(com.lightgraph.raft.proto.HeartBeatRequest request,
            io.grpc.stub.StreamObserver<com.lightgraph.raft.proto.HeartBeatResponse> responseObserver) {
        HeartBeatResponse.Builder builder = HeartBeatResponse.newBuilder();
        long requestTerm = request.getTerm();
        ConsensusInstance requestLeader = ConsensusInstance
                .getInstance(request.getLeader().toByteArray(), ConsensusInstance.class);
        synchronized (stateMachine) {
            try {
                if (voteFor != null && requestLeader.equals(voteFor)) {
                    stateMachine.onEvent(RaftEvent.DISCOVERY_CURRENT_LEADER);
                    this.term = requestTerm;
                    setLeader(voteFor);
                    voteFor = null;
                }
                long commitIndex = request.getCommitIndex();
                if (requestTerm > term) {
                    stateMachine.onEvent(RaftEvent.DISCOVERY_HIGH_TERM);
                    resetTimeOut();
                    logSystem.setCommitedEditIndex(commitIndex);
                    this.term = requestTerm;
                    setLeader(requestLeader);
                } else if (requestTerm == term && requestLeader.equals(this.leader)) {
                    logSystem.setCommitedEditIndex(commitIndex);
                    resetTimeOut();
                }
                builder.setIndex(logSystem.getLatestEditIndex());
            } catch (Throwable t) {
                LOG.error("handle hartbeat failed!", t);
            }
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    public ConsensusInstance getLeader() {
        return leader;
    }

    public void resetElectionTimeout() {
        if (electionTimeOutCheckTask == null) {
            return;
        }
        long delay = (long) (electionTimeout + Math.random() * electionTimeout);
        electionTimeOutCheckTask.setDelay(delay);
        electionTimeOutCheckTask.reset();
    }

    public void resetHeartBeatCheckTimeout() {
        if (heartBeatCheckTask == null) {
            return;
        }
        heartBeatCheckTask.setDelay(heartBeatTimeout);
        heartBeatCheckTask.reset();
    }

    public void resetHeartBeatBoardCastTask() {
        if (heartBeatBoardCastTask == null) {
            return;
        }
        heartBeatBoardCastTask.setDelay(heartBeatTimeout / 3);
        heartBeatBoardCastTask.reset();
    }

    public void resetTimeOut() {
        resetElectionTimeout();
        resetHeartBeatCheckTimeout();
    }

    public void sendVoteRequest() {
        this.term++;
        VoteRequest.Builder builder = VoteRequest.newBuilder();
        builder.setCandidate(ByteString.copyFrom(instance.getBytes(ConsensusInstance.class)));
        builder.setTerm(term);
        builder.setIndex(logSystem.getLatestEditIndex());
        votes.set(1);
        if (instance.getGroupSize() == 1) {
            boolean ret = stateMachine.onEvent(RaftEvent.RECEIVE_VOTES_FROM_MAJORITY_SERVERS);
            if (ret) {
                setLeader(RaftQuorum.this.instance);
            }
        } else if (instance.getGroupSize() > 1) {
            Iterable<ConsensusInstance> instances = instance.getOtherMember();
            for (ConsensusInstance instance : instances) {
                Node target = instance.getLocation();
                builder.setInstance(ByteString.copyFrom(instance.getBytes(ConsensusInstance.class)));
                VoteRequest request = builder.build();
                module.getPeer(target).vote(request, new StreamObserver<VoteResponse>() {
                    @Override
                    public void onNext(VoteResponse voteResponse) {
                        boolean accept = voteResponse.getAccept();
                        if (accept) {
                            synchronized (stateMachine) {
                                int curVotes = votes.incrementAndGet();
                                if (curVotes > instance.getGroupSize() / 2
                                        && stateMachine.getCurrentState() != RaftState.LEADER) {
                                    stateMachine.onEvent(RaftEvent.RECEIVE_VOTES_FROM_MAJORITY_SERVERS);
                                    setLeader(RaftQuorum.this.instance);
                                }
                            }
                        }
                    }

                    @Override
                    public void onError(Throwable throwable) {

                    }

                    @Override
                    public void onCompleted() {

                    }
                });
            }
        }
    }

    public boolean appendLogSync(ConsensusInstance instance, long preTerm, long term, long index, byte[] data) {
        if (!this.isLeader()) {
            return false;
        }
        AppendLogEditRequest.Builder builder = AppendLogEditRequest.newBuilder();
        builder.setTerm(term);
        builder.setPreTerm(preTerm);
        builder.setIndex(index);
        builder.setRecord(ByteString.copyFrom(data));
        builder.setInstance(ByteString.copyFrom(instance.getBytes(ConsensusInstance.class)));
        AppendLogEditRequest request = builder.build();
        AppendLogEditResponse response = module.getSyncPeer(instance.getLocation()).appendLog(request);
        return response.getIsok();
    }

    public class RaftClusterMetricsTask implements Task {

        @Override
        public String description() {
            return String.format("raft-cluster-metrics-task%s", instance.getDescription());
        }

        @Override
        public Object doTask() {
            if (leader == null) {
                LOG.info(String.format("instance%s\tstate:%s\tterm:%d\tleader:%s",
                        instance.getDescription(), stateMachine.getCurrentState(), term, "Not Found"));
            } else {
                LOG.info(String.format("instance%s\tstate:%s\tterm:%d\tleader:%s",
                        instance.getDescription(), stateMachine.getCurrentState(), term, leader.getDescription()));
            }
            raftClusterMetricsTask.setDelay(10000);
            raftClusterMetricsTask.reset();
            return null;
        }
    }

    public class HeartBeatBoardCastTask implements Task {

        @Override
        public String description() {
            return String.format("heart-heat-boardcast-task[%s]", instance.getDescription());
        }

        @Override
        public Object doTask() {
            if (!isLeader()) {
                return null;
            }
            resetHeartBeatBoardCastTask();
            HeartBeatRequest.Builder builder = HeartBeatRequest.newBuilder();
            builder.setIsHeartBeat(true);
            builder.setTerm(term);
            builder.setLeader(ByteString.copyFrom(instance.getBytes(ConsensusInstance.class)));
            builder.setIndex(logSystem.getLatestEditIndex());
            builder.setCommitIndex(logSystem.getCommitedEditIndex());
            Iterable<ConsensusInstance> instances = instance.getOtherMember();
            for (ConsensusInstance instance : instances) {
                Node target = instance.getLocation();
                builder.setInstance(ByteString.copyFrom(instance.getBytes(ConsensusInstance.class)));
                HeartBeatRequest request = builder.build();
                module.getPeer(target).keepHeartBeat(request, new StreamObserver<HeartBeatResponse>() {
                    @Override
                    public void onNext(HeartBeatResponse heartBeatResponse) {
                        long index = heartBeatResponse.getIndex();
                        logSystem.setNodeIndex(instance, index);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        logSystem.removeNodeIndex(instance);
                        LOG.error("boardcast failed to " + target, throwable);
                    }

                    @Override
                    public void onCompleted() {

                    }
                });
            }
            return null;
        }
    }

    public class HeartBeatCheckTask implements Task {

        @Override
        public String description() {
            return String.format("heart-heat-check-task[%s]", instance.getDescription());
        }

        @Override
        public Object doTask() {
            resetHeartBeatCheckTimeout();
            synchronized (stateMachine) {
                if (stateMachine.getCurrentState() == RaftState.FOLLOWER) {
                    stateMachine.onEvent(RaftEvent.HEARTBEAT_TIMEOUT);
                }
            }
            return null;
        }
    }

    public class ElectionTimeOutCheckTask implements Task {

        @Override
        public String description() {
            return String.format("election-task[%s]", instance.getDescription());
        }

        @Override
        public Object doTask() {
            synchronized (stateMachine) {
                if (stateMachine.getCurrentState() == RaftState.CANDIDATE) {
                    stateMachine.onEvent(RaftEvent.CANDIDATE_ELECTION_TIMEOUT);
                }
            }
            return null;
        }
    }
}
