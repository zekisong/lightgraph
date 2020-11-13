package com.lightgraph.graph.command;

import com.lightgraph.graph.modules.consensus.ConsensusIO;
import com.lightgraph.graph.modules.consensus.WriteFuture;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class CommandRunner implements Runnable {

    private static final Log LOG = LogFactory.getLog(CommandRunner.class);
    protected ConsensusIO consensusIO;
    protected CommandObserver observer;

    public CommandRunner(ConsensusIO consensusIO, CommandObserver observer) {
        this.consensusIO = consensusIO;
        this.observer = observer;
    }

    public WriteFuture writeCommand(byte[] data) {
        return consensusIO.write(data);
    }

    @Override
    public void run() {
        while (true) {
            try {
                byte[] data = consensusIO.read();
                if (observer != null) {
                    observer.preRun();
                }
                runCommand(data);
                if (observer != null) {
                    observer.afterRun(data);
                }
            } catch (Throwable t) {
                LOG.error("run command failed!", t);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    abstract void runCommand(byte[] r);

}
