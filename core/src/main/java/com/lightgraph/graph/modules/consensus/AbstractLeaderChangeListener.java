package com.lightgraph.graph.modules.consensus;

public abstract class AbstractLeaderChangeListener<T extends ConsensusInstance> implements LeaderChangeListener {
    protected ConsensusInstance instance;

    @Override
    public void bindInstance(ConsensusInstance instance) {
        this.instance = instance;
    }
}
