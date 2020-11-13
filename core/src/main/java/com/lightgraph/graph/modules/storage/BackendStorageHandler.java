package com.lightgraph.graph.modules.storage;

import java.io.IOException;
import java.util.List;

public interface BackendStorageHandler {
    void open() throws IOException;

    void close() throws IOException;

    void set(KeyValue keyValue) throws IOException;

    void delete(byte[] k) throws IOException;

    KeyValue get(KeyValue keyValue) throws IOException;

    List<KeyValue> scan(KeyValue keyValue) throws IOException;

    List<KeyValue> multiGet(List<byte[]> k) throws IOException;
}
