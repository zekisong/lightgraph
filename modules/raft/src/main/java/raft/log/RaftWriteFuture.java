package raft.log;

import com.lightgraph.graph.modules.consensus.WriteFuture;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.CountDownLatch;

public class RaftWriteFuture implements WriteFuture {
    private static final Log LOG = LogFactory.getLog(RaftWriteFuture.class);
    private long term;
    private long index;
    private volatile boolean success;
    private CountDownLatch latch = new CountDownLatch(1);

    public RaftWriteFuture(long term, long index) {
        this.term = term;
        this.index = index;
    }

    @Override
    public boolean get() {
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return success;
    }

    public long getTerm() {
        return term;
    }

    public long getIndex() {
        return index;
    }

    public void setSuccess(boolean success) {
        this.success = success;
        latch.countDown();
    }
}