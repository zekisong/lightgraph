package com.lightgraph.graph.storage;

import com.lightgraph.graph.cluster.Replication;
import com.lightgraph.graph.modules.consensus.AbstractLeaderChangeListener;
import com.lightgraph.graph.modules.consensus.ConsensusInstance;
import com.lightgraph.graph.modules.consensus.ConsensusInstanceState;
import com.lightgraph.graph.modules.rpc.MetaRpcService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ReplicationListener extends AbstractLeaderChangeListener {

    private static Log LOG = LogFactory.getLog(ReplicationListener.class);
    private MetaRpcService metaRpcService;

    public ReplicationListener(MetaRpcService metaRpcService) {
        this.metaRpcService = metaRpcService;
    }

    @Override
    public void onLeaderChangeEvent(long term, ConsensusInstance leader) {
        if (leader == null) {
            this.instance.setState(ConsensusInstanceState.UNSTABLE);
        } else if (this.instance.equals(leader)) {
            this.instance.setState(ConsensusInstanceState.LEADER);
        } else {
            this.instance.setState(ConsensusInstanceState.FOLLOWER);
        }
        try {
            boolean ret = metaRpcService.updateReplication((Replication) this.instance);
            if (ret) {
                LOG.info(this.instance.getDescription() + "\tstate:" + this.instance.getState()
                        + "\tupdate succcessful!");
            } else {
                LOG.info(this.instance.getDescription() + "\tstate:" + this.instance.getState() + "\tupdate failed!");
            }
        } catch (Throwable t) {
            LOG.error("update replication failed!", t);
        }
    }

    public Replication getReplication() {
        return (Replication) this.instance;
    }
}
