package com.lightgraph.graph.settings;

import com.lightgraph.graph.config.Configurable;
import com.lightgraph.graph.utils.ByteUtils;
import com.lightgraph.graph.utils.DataTypeUtils;
import com.lightgraph.graph.writable.Sizeable;
import com.lightgraph.graph.writable.Writable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class Setting implements Configurable, Writable, Sizeable {
    private Map<String, String> config = new ConcurrentHashMap();
    private long lastModifyTime;
    private long version;

    public Setting() {
    }

    public Setting(byte[] bytes) {
        int pos = ByteUtils.RESERVED_BYTE_SIZE＿FOR_TX;
        int kvCount = ByteUtils.getInt(bytes, pos);
        pos = pos + ByteUtils.SIZE_INT;
        for (int i = 0; i < kvCount; i++) {
            int keySize = ByteUtils.getInt(bytes, pos);
            pos = pos + ByteUtils.SIZE_INT;
            int valueSize = ByteUtils.getInt(bytes, pos);
            pos = pos + ByteUtils.SIZE_INT;
            String key = ByteUtils.getString(bytes, pos, keySize);
            pos = pos + keySize;
            String value = "";
            if (valueSize > 0)
                value = ByteUtils.getString(bytes, pos, valueSize);
            pos = pos + valueSize;
            config.put(key, value);
        }
        this.lastModifyTime = ByteUtils.getLong(bytes, pos);
        pos = pos + ByteUtils.SIZE_LONG;
        this.version = ByteUtils.getLong(bytes, pos);
    }

    public void set(String key, String value) {
        config.put(key, value);
    }

    public <T> T get(String key, T defaultValue) {
        String value = config.get(key);
        if (value == null)
            return defaultValue;
        Class clazz = defaultValue.getClass();
        T ret = DataTypeUtils.getValue(value, clazz);
        return ret;
    }

    public long getLastModifyTime() {
        return lastModifyTime;
    }

    public void setLastModifyTime(long lastModifyTime) {
        this.lastModifyTime = lastModifyTime;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public Map<String, String> getConfig() {
        return config;
    }

    @Override
    public int size() {
        int size = 0;
        for (String key : config.keySet()) {
            size = size + key.length();
            int valueSize = 0;
            if (config.get(key) != null)
                valueSize = config.get(key).length();
            size = size + valueSize;
        }
        size = ByteUtils.RESERVED_BYTE_SIZE＿FOR_TX
                + ByteUtils.SIZE_INT  //config kv count
                + ByteUtils.SIZE_INT * 2 * config.size()   //each kv size
                + size  //each kv
                + ByteUtils.SIZE_LONG //lastModifyTime
                + ByteUtils.SIZE_LONG; //version
        return size;
    }

    @Override
    public byte[] getBytes() {
        int size = this.size();
        byte[] data = new byte[size];
        int pos = ByteUtils.RESERVED_BYTE_SIZE＿FOR_TX;
        pos = ByteUtils.putInt(data, pos, config.size());
        for (String key : config.keySet()) {
            int keySize = key.length();
            int valueSize = 0;
            if (config.get(key) != null)
                valueSize = config.get(key).length();
            pos = ByteUtils.putInt(data, pos, keySize);
            pos = ByteUtils.putInt(data, pos, valueSize);
            pos = ByteUtils.putBytes(data, pos, key.getBytes());
            if (valueSize > 0) {
                pos = ByteUtils.putBytes(data, pos, config.get(key).getBytes());
            }
        }
        pos = ByteUtils.putLong(data, pos, lastModifyTime);
        ByteUtils.putLong(data, pos, version);
        return data;
    }
}
