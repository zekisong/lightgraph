package raft.stm;

public enum RaftState {
    FOLLOWER,
    CANDIDATE,
    LEADER
}