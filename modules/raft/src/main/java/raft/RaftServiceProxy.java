package raft;

import com.lightgraph.graph.modules.consensus.ConsensusInstance;
import com.lightgraph.raft.proto.RaftServiceGrpc;

public class RaftServiceProxy extends RaftServiceGrpc.RaftServiceImplBase {

    private ConsensusModuleImpl module;

    public RaftServiceProxy(ConsensusModuleImpl module) {
        this.module = module;
    }

    public void vote(com.lightgraph.raft.proto.VoteRequest request,
                     io.grpc.stub.StreamObserver<com.lightgraph.raft.proto.VoteResponse> responseObserver) {
        ConsensusInstance instance = new ConsensusInstance(request.getInstance().toByteArray());
        RaftQuorum quorum = module.getRaftQuorum(instance);
        if (quorum != null) {
            quorum.vote(request, responseObserver);
        } else {
            responseObserver.onCompleted();
        }
    }

    public void keepHeartBeat(com.lightgraph.raft.proto.HeartBeatRequest request,
                              io.grpc.stub.StreamObserver<com.lightgraph.raft.proto.HeartBeatResponse> responseObserver) {
        ConsensusInstance instance = new ConsensusInstance(request.getInstance().toByteArray());
        RaftQuorum quorum = module.getRaftQuorum(instance);
        if (quorum != null) {
            quorum.keepHeartBeat(request, responseObserver);
        } else {
            responseObserver.onCompleted();
        }
    }

    public void appendLog(com.lightgraph.raft.proto.AppendLogEditRequest request,
                          io.grpc.stub.StreamObserver<com.lightgraph.raft.proto.AppendLogEditResponse> responseObserver) {
        ConsensusInstance instance = new ConsensusInstance(request.getInstance().toByteArray());
        RaftQuorum quorum = module.getRaftQuorum(instance);
        if (quorum != null) {
            quorum.appendLog(request, responseObserver);
        } else {
            responseObserver.onCompleted();
        }
    }
}
