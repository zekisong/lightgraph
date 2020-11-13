package com.lightgraph.graph.meta;

import com.lightgraph.graph.constant.GraphConstant;
import com.lightgraph.graph.exception.GraphException;
import com.lightgraph.graph.meta.cluster.GraphMeta;
import com.lightgraph.graph.modules.storage.BackendStorageHandler;
import com.lightgraph.graph.modules.storage.KeyValue;
import com.lightgraph.graph.utils.ByteUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

public class MetaManager {

    private static Log LOG = LogFactory.getLog(MetaManager.class);
    private AtomicLong idAssginer = new AtomicLong();
    private BackendStorageHandler metaStorage;

    public MetaManager(BackendStorageHandler metaStorage) {
        this.metaStorage = metaStorage;
        CompletableFuture.supplyAsync(() -> {
            while (true) {
                try {
                    byte[] maxID = metaStorage.get(new KeyValue(GraphConstant.META_ID_KEY.getBytes(), "".getBytes())).getValue();
                    if (maxID != null) {
                        idAssginer.set(ByteUtils.getLong(maxID, 0));
                    } else {
                        idAssginer.set(0);
                    }
                    break;
                } catch (Exception e) {
                    try {
                        LOG.warn("meta storage not prepare, sleep 1s and retry to connect...");
                        Thread.sleep(1000);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                }
            }
            return null;
        });
    }

    public synchronized long saveMeta(ElementMeta meta) {
        long id = aquireID();
        try {
            byte[] maxID = new byte[ByteUtils.SIZE_LONG];
            ByteUtils.putLong(maxID, 0, id);
            metaStorage.set(new KeyValue(GraphConstant.META_ID_KEY.getBytes(), "".getBytes(), maxID));
        } catch (IOException e) {
            throw new GraphException("save meta failed!", e);
        }
        meta.setId(id);
        meta.setCreateTime(System.currentTimeMillis());
        for (KeyValue kv : meta.toKVS()) {
            try {
                metaStorage.set(kv);
            } catch (IOException e) {
                e.printStackTrace();
                return -1;
            }
        }
        return id;
    }

    public GraphMeta getGraphMeta(String name) {
        List<KeyValue> keyValues = null;
        try {
            keyValues = metaStorage.scan(GraphMeta.getKey(name));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new GraphMeta(keyValues);
    }

    public VertexMeta getVertexMeta(String graph, String name) {
        List<KeyValue> keyValues = null;
        try {
            keyValues = metaStorage.scan(VertexMeta.getKey(graph, name));
            if (keyValues != null && keyValues.size() > 0) {
                return new VertexMeta(keyValues);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public EdgeMeta getEdgeMeta(String graph, String name) {
        List<KeyValue> keyValues = null;
        try {
            keyValues = metaStorage.scan(EdgeMeta.getKey(graph, name));
            if (keyValues != null && keyValues.size() > 0) {
                return new EdgeMeta(keyValues);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public synchronized long aquireID() {
        return idAssginer.incrementAndGet();
    }

    public List<GraphMeta> listGraphMeta() {
        List<GraphMeta> metas = new ArrayList<>();
        try {
            List<KeyValue> keyValues = metaStorage.scan(GraphMeta.getKey(""));
            Map<String, List<KeyValue>> graphMap = new HashMap();
            keyValues.stream().forEach(keyValue -> {
                String graphName = GraphMeta.getGraphName(keyValue.getKey());
                if (graphMap.containsKey(graphName)) {
                    graphMap.get(graphName).add(keyValue);
                } else {
                    List<KeyValue> kvs = new ArrayList<>();
                    kvs.add(keyValue);
                    graphMap.put(graphName, kvs);
                }
            });
            for (List<KeyValue> kvs : graphMap.values()) {
                metas.add(new GraphMeta(kvs));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return metas;
    }
}
