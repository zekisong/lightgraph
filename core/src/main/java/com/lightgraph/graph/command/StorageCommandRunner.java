package com.lightgraph.graph.command;

import com.lightgraph.graph.modules.consensus.ConsensusIO;
import com.lightgraph.graph.modules.storage.BackendStorageHandler;
import com.lightgraph.graph.modules.storage.Batch;
import com.lightgraph.graph.modules.storage.Key;
import com.lightgraph.graph.modules.storage.KeyValue;
import com.lightgraph.graph.utils.SerdeUtils;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.List;

public class StorageCommandRunner extends CommandRunner {

    private BackendStorageHandler handler;

    public StorageCommandRunner(ConsensusIO consensusIO, CommandObserver observer, BackendStorageHandler handler) {
        super(consensusIO, observer);
        this.handler = handler;
    }

    public KeyValue get(Key key) throws IOException {
        return handler.get(key);
    }

    public Iterator<KeyValue> scan(Key start) throws IOException {
        return handler.scan(start);
    }

    @Override
    public void runCommand(byte[] r) {
        StorageOperation op = StorageOperation.valueOf(r[0]);
        switch (op) {
            case PUT:
                KeyValue kv = KeyValue.getInstance(r, 1, r.length - 1, KeyValue.class);
                try {
                    handler.set(kv);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            case BATCH_PUT:
                Batch batch = Batch.getInstance(r, 1, r.length - 1, Batch.class);
                try {
                    handler.batchSet(batch.getBatch());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            case DELETE:
                break;
        }
    }
}
