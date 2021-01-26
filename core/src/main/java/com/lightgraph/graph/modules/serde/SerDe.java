package com.lightgraph.graph.modules.serde;

public interface SerDe {

    <T> byte[] serialize(T object);

    <T> byte[] serialize(T object, Class<T> clazz);

    <T> T deSerialize(byte[] data, Class<T> clazz);

    <T> T deSerialize(byte[] data, int offset, int length, Class<T> clazz);
}
