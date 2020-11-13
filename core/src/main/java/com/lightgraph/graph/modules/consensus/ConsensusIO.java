package com.lightgraph.graph.modules.consensus;

public interface ConsensusIO {
    WriteFuture write(byte[] record);

    byte[] read();
}
