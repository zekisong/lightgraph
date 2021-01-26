package com.lightgraph.graph.modules.storage;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public interface BackendStorageHandler {

    void open() throws IOException;

    void close() throws IOException;

    void set(KeyValue keyValue) throws IOException;

    void batchSet(List<KeyValue> keyValues) throws IOException;

    void delete(byte[] k) throws IOException;

    KeyValue get(Key key) throws IOException;

    Iterator<KeyValue> scan(Key start) throws IOException;

    List<KeyValue> multiGet(List<byte[]> k) throws IOException;
}
