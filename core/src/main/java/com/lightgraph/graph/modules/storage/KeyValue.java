package com.lightgraph.graph.modules.storage;

import com.lightgraph.graph.writable.Sizeable;
import com.lightgraph.graph.writable.Writable;
import com.lightgraph.graph.utils.ByteUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class KeyValue implements Writable, Sizeable {
    byte[] key;
    byte[] secKey;
    byte[] value;

    public KeyValue(byte[] bytes) {
        int pos = ByteUtils.RESERVED_BYTE_SIZE＿FOR_TX;
        int keySize = ByteUtils.getInt(bytes, pos);
        pos = pos + ByteUtils.SIZE_INT;
        int secKeySize = ByteUtils.getInt(bytes, pos);
        pos = pos + ByteUtils.SIZE_INT;
        this.key = ByteUtils.getBytes(bytes, pos, keySize);
        pos = pos + keySize;
        if (secKeySize > 0) {
            this.secKey = ByteUtils.getBytes(bytes, pos, secKeySize);
            pos = pos + secKeySize;
        }
        if (pos < bytes.length) {
            this.value = ByteUtils.getBytes(bytes, pos, bytes.length - pos);
        }
    }

    public KeyValue(byte[] key, byte[] secKey) {
        this.key = key;
        this.secKey = secKey;
    }

    public KeyValue(byte[] key, byte[] secKey, byte[] value) {
        this.key = key;
        this.secKey = secKey;
        this.value = value;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getSecKey() {
        return secKey;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public int size() {
        return ByteUtils.RESERVED_BYTE_SIZE＿FOR_TX
                + ByteUtils.SIZE_INT
                + ByteUtils.SIZE_INT
                + key.length
                + (secKey == null ? 0 : secKey.length)
                + (value == null ? 0 : value.length);
    }

    @Override
    public byte[] getBytes() {
        int size = size();
        byte[] data = new byte[size];
        int pos = ByteUtils.RESERVED_BYTE_SIZE＿FOR_TX;
        pos = ByteUtils.putInt(data, pos, key.length);
        pos = ByteUtils.putInt(data, pos, secKey.length);
        pos = ByteUtils.putBytes(data, pos, key);
        if (secKey != null) {
            pos = ByteUtils.putBytes(data, pos, secKey);
        }
        if (value != null) {
            ByteUtils.putBytes(data, pos, value);
        }
        return data;
    }

    @Override
    public String toString() {
        String result = "";
        if (key != null) {
            result = result + new String(key);
        }
        if (secKey != null) {
            result = result + "_" + new String(secKey);
        }
        if (value != null) {
            result = result + ":" + new String(value);
        }
        return result;
    }
}
