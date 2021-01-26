package com.lightgraph.graph.modules.consensus;


public interface ConsensusHandler extends ConsensusIO {

    void addLeaderChangeListener(LeaderChangeListener callBack);

    boolean isLeader();
}
