package com.lightgraph.graph.config;

import com.lightgraph.graph.utils.DataTypeUtils;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GraphConfig implements Configurable {

    private static final Log LOG = LogFactory.getLog(GraphConfig.class);
    private static volatile GraphConfig INSTANCE = null;
    private Map<String, String> config = new ConcurrentHashMap();

    public GraphConfig() {
        final Properties properties = new Properties();
        try {
            properties.load(this.getClass().getClassLoader().getResourceAsStream("graph.properties"));
            for (Object property : properties.keySet()) {
                config.put(property.toString(), properties.get(property).toString());
            }
        } catch (Throwable e) {
            LOG.warn("load config from graph.properties failed!", e);
        }
    }

    public static GraphConfig getInstance() {
        if (INSTANCE == null) {
            synchronized (GraphConfig.class) {
                if (INSTANCE == null) {
                    INSTANCE = new GraphConfig();
                }
            }
        }
        return INSTANCE;
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
}
