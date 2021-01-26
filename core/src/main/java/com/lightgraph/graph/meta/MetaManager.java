package com.lightgraph.graph.meta;

import com.lightgraph.graph.constant.GraphConstant;
import com.lightgraph.graph.exception.GraphException;
import com.lightgraph.graph.meta.cluster.GraphMeta;
import com.lightgraph.graph.modules.storage.BackendStorageHandler;
import com.lightgraph.graph.modules.storage.Key;
import com.lightgraph.graph.modules.storage.KeyValue;
import com.lightgraph.graph.utils.ByteUtils;
import java.util.stream.Stream;
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
                    byte[] maxID = metaStorage
                            .get(new Key(0, GraphConstant.META_ID_KEY.getBytes()))
                            .getValue();
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

    public synchronized long saveGraphMeta(ElementMeta meta) {
        long id = aquireID();
        try {
            metaStorage.set(new KeyValue(GraphConstant.META_ID_KEY.getBytes(), ByteUtils.longToBytes(id)));
        } catch (IOException e) {
            throw new GraphException("save meta failed!", e);
        }
        meta.setId(id);
        meta.setCreateTime(System.currentTimeMillis());
        try {
            for (KeyValue kv : meta.toMutations()) {
                metaStorage.set(kv);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
        return id;
    }

    public synchronized long saveLabelMeta(LabelMeta meta) {
        long labelId = aquireID();
        long maxId = labelId;
        long time = System.currentTimeMillis();
        for (PropertyMeta pm : meta.getProperties()) {
            maxId = aquireID();
            pm.setId(maxId);
            pm.setCreateTime(time);
        }
        try {
            byte[] maxID = new byte[ByteUtils.SIZE_LONG];
            ByteUtils.putLong(maxID, 0, maxId);
            metaStorage.set(new KeyValue(GraphConstant.META_ID_KEY.getBytes(), maxID));
        } catch (IOException e) {
            throw new GraphException("save meta failed!", e);
        }
        meta.setId(labelId);
        meta.setCreateTime(time);
        try {
            for (KeyValue kv : meta.toMutations()) {
                metaStorage.set(kv);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
        return labelId;
    }

    public GraphMeta getGraphMeta(String name) {
        KeyValue keyValue = null;
        try {
            keyValue = metaStorage.get(GraphMeta.makeKey(name));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return GraphMeta.getInstance(keyValue, GraphMeta.class);
    }

    public LabelMeta getLabelMetaById(Long id, LabelType type) {
        KeyValue keyValue;
        try {
            Key indexKey = LabelMeta.getIndexKey(id);
            KeyValue indexKv = metaStorage.get(indexKey);
            keyValue = metaStorage.get(Key.warp(indexKv.getValue()));
            if (keyValue.getValue() == null) {
                return null;
            } else {
                switch (type) {
                    case EDGE:
                        return EdgeMeta.getInstance(keyValue, EdgeMeta.class);
                    case VERTEX:
                        return VertexMeta.getInstance(keyValue, VertexMeta.class);
                    default:
                        throw new RuntimeException("label type not support!");
                }
            }
        } catch (IOException e) {
            throw new GraphException("get label meta failed!", e);
        }
    }

    public VertexMeta getVertexMeta(Long graphId, String name) {
        KeyValue keyValue;
        try {
            keyValue = metaStorage.get(LabelMeta.makeKey(MetaType.VERTEX, graphId, name));
            if (keyValue.getValue() == null) {
                return null;
            } else {
                return VertexMeta.getInstance(keyValue, VertexMeta.class);
            }
        } catch (IOException e) {
            throw new GraphException("get vertex meta failed!", e);
        }
    }

    public EdgeMeta getEdgeMeta(Long graphId, String name) {
        KeyValue keyValue;
        try {
            keyValue = metaStorage.get(EdgeMeta.makeKey(MetaType.EDGE, graphId, name));
            if (keyValue.getValue() == null) {
                return null;
            } else {
                return EdgeMeta.getInstance(keyValue, EdgeMeta.class);
            }
        } catch (IOException e) {
            throw new GraphException("get edge meta failed!", e);
        }
    }

    public synchronized long aquireID() {
        return idAssginer.incrementAndGet();
    }

    public List<GraphMeta> listGraphMeta() {
        List<GraphMeta> metas = new ArrayList<>();
        try {
            Iterator<KeyValue> it = metaStorage.scan(GraphMeta.makeGraphScanKey());
            List<KeyValue> kvs = new ArrayList<>();
            while (it.hasNext()) {
                kvs.add(it.next());
            }
            kvs.stream().forEach(keyValue -> {
                GraphMeta meta = GraphMeta.getInstance(keyValue, GraphMeta.class);
                metas.add(meta);
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        return metas;
    }

    public List<LabelMeta> listLabelMeta(String graph, MetaType metaType) {
        List<LabelMeta> metas = new ArrayList<>();
        try {
            GraphMeta graphMeta = getGraphMeta(graph);
            Iterator<KeyValue> it = metaStorage.scan(LabelMeta.makeLabelScanKey(metaType, graphMeta.getId()));
            List<KeyValue> kvs = new ArrayList<>();
            while (it.hasNext()) {
                kvs.add(it.next());
            }
            kvs.stream().forEach(keyValue -> {
                switch (metaType) {
                    case EDGE:
                        EdgeMeta em = EdgeMeta.getInstance(keyValue, EdgeMeta.class);
                        metas.add(em);
                        break;
                    case VERTEX:
                        VertexMeta vm = VertexMeta.getInstance(keyValue, VertexMeta.class);
                        metas.add(vm);
                        break;
                    default:
                        throw new GraphException("not support meta type!");
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        return metas;
    }


}
