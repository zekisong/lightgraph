package com.lightgraph.graph.modules.storage;

import com.lightgraph.graph.writable.Writable;

public class KeyValue extends Writable {

    private Key key;
    private byte[] value;

    public KeyValue() {
    }

    public KeyValue(byte[] key) {
        this.key = new Key(0, key);
    }

    public KeyValue(byte[] key, byte[] value) {
        this.key = new Key(0, key);
        this.value = value;
    }

    public KeyValue(Key key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public Key getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    @Override
    public String toString() {
        String result = "";
        if (key != null) {
            result = result + key.toString();
        }
        if (value != null) {
            result = result + ":" + new String(value);
        }
        return result;
    }
}
