package com.lightgraph.graph.settings;

import com.lightgraph.graph.config.Configurable;
import com.lightgraph.graph.utils.DataTypeUtils;
import com.lightgraph.graph.writable.Writable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class Setting extends Writable implements Configurable {

    private Map<String, String> config = new ConcurrentHashMap();
    private long lastModifyTime;
    private long version;

    public Setting() {
    }

    public void set(String key, String value) {
        config.put(key, value);
    }

    public <T> T get(String key, T defaultValue) {
        String value = config.get(key);
        if (value == null) {
            return defaultValue;
        }
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
}
