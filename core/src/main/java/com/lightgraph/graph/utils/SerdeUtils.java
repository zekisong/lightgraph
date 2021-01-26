package com.lightgraph.graph.utils;

import com.lightgraph.graph.config.GraphConfig;
import com.lightgraph.graph.constant.GraphConstant;
import com.lightgraph.graph.modules.serde.SerDe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SerdeUtils {

    private static final Log LOG = LogFactory.getLog(SerdeUtils.class);

    protected static SerDe serDe;

    static {
        GraphConfig conf = GraphConfig.getInstance();
        String clazz = conf.get(GraphConstant.GRAPH_SERIALIZE_CLAZZ, GraphConstant.GRAPH_SERIALIZE_CLAZZ_DEFAULT);
        try {
            serDe = ReflectUtils.getInstance(clazz);
        } catch (Exception e) {
            LOG.info("init serde plugin failed!", e);
        }
    }

    public static <T> byte[] getBytes(T object) {
        return serDe.serialize(object);
    }

    public static <T> byte[] getBytes(T object, Class<T> clazz) {
        return serDe.serialize(object, clazz);
    }

    public static <T> T getObject(byte[] bytes, Class<T> clazz) {
        return serDe.deSerialize(bytes, clazz);
    }

    public static <T> T getObject(byte[] bytes, int offset, int length, Class<T> clazz) {
        return serDe.deSerialize(bytes, offset, length, clazz);
    }
}
