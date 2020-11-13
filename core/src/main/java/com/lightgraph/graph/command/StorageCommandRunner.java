package com.lightgraph.graph.command;

import com.lightgraph.graph.modules.consensus.ConsensusIO;
import com.lightgraph.graph.modules.storage.BackendStorageHandler;
import com.lightgraph.graph.modules.storage.KeyValue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.List;

public class StorageCommandRunner extends CommandRunner {

    private static final Log LOG = LogFactory.getLog(StorageCommandRunner.class);
    private BackendStorageHandler handler;

    public StorageCommandRunner(ConsensusIO consensusIO, CommandObserver observer, BackendStorageHandler handler) {
        super(consensusIO, observer);
        this.handler = handler;
    }

    public KeyValue get(KeyValue keyValue) throws IOException {
        return handler.get(keyValue);
    }

    public List<KeyValue> scan(KeyValue keyValue) throws IOException {
        return handler.scan(keyValue);
    }

    @Override
    public void runCommand(byte[] r) {
        LOG.info("command ===>" + new String(r));
        StorageOperation op = StorageOperation.valueOf(r[0]);
        switch (op) {
            case PUT:
                KeyValue kv = new KeyValue(r);
                try {
                    handler.set(kv);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            case DELETE:
                break;
        }
    }
}
