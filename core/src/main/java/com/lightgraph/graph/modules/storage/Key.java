package com.lightgraph.graph.modules.storage;

import com.lightgraph.graph.utils.ByteUtils;
import com.lightgraph.graph.utils.SerdeUtils;
import java.nio.ByteBuffer;

public class Key {

    private byte[] key;

    public Key() {
    }

    public Key(int routingIndex, Object... keys) {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        ByteUtils.writeVarint(routingIndex, buffer);
        for (Object key : keys) {
            byte[] bytes = SerdeUtils.getBytes(key);
            ByteUtils.writeVarint(bytes.length, buffer);
            buffer.put(bytes);
        }
        buffer.flip();
        key = new byte[buffer.limit()];
        buffer.get(key);
    }

    public byte[] getIndex(int index) {
        ByteBuffer buffer = ByteBuffer.wrap(key);
        ByteUtils.readVarint(buffer);
        int len = 0;
        for (int i = 0; i < index + 1; i++) {
            buffer.position(buffer.position() + len);
            len = ByteUtils.readVarint(buffer);
        }
        byte[] result = new byte[len];
        buffer.get(result);
        return result;
    }

    public byte[] getRoutingBytes() {
        ByteBuffer buffer = ByteBuffer.wrap(key);
        int routingIndex = ByteUtils.readVarint(buffer);
        for (int i = 0; i < routingIndex + 1; i++) {
            int len = ByteUtils.readVarint(buffer);
            buffer.position(buffer.position() + len);
        }
        buffer.flip();
        byte[] result = new byte[buffer.limit()];
        buffer.get(result);
        return result;
    }

    public byte[] bytes() {
        return key;
    }

    public static Key warp(byte[] key) {
        Key k = new Key();
        k.key = key;
        return k;
    }

    public String toString() {
        return new String(key);
    }
}
