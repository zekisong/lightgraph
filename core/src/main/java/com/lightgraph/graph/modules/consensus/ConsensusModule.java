package com.lightgraph.graph.modules.consensus;

import com.lightgraph.graph.modules.Module;
import com.lightgraph.graph.modules.NetworkModule;

public interface ConsensusModule extends Module {

    void bindNetwork(NetworkModule networkModule);

    ConsensusHandler loadInstance(ConsensusInstance instance, boolean clean, LeaderChangeListener... listener);

    void unLoadInstance(ConsensusGroup group);

    ConsensusInstance getGroupLeader(ConsensusInstance instance);

    ConsensusHandler getConsensusHandler();
}
