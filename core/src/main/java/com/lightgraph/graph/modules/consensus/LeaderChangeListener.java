package com.lightgraph.graph.modules.consensus;

public interface LeaderChangeListener {

    void onLeaderChangeEvent(long term, ConsensusInstance leader);

    void bindInstance(ConsensusInstance instance);
}
