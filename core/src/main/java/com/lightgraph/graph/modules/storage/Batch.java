package com.lightgraph.graph.modules.storage;

import com.lightgraph.graph.writable.Writable;
import java.util.List;

public class Batch extends Writable {

    List<KeyValue> batch;

    public Batch() {
    }

    public Batch(List<KeyValue> batch) {
        this.batch = batch;
    }

    public List<KeyValue> getBatch() {
        return batch;
    }

    public void setBatch(List<KeyValue> batch) {
        this.batch = batch;
    }
}
