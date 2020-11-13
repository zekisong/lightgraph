package com.lightgraph.graph.meta.cluster;

import com.lightgraph.graph.constant.GraphConstant;
import com.lightgraph.graph.meta.ElementMeta;
import com.lightgraph.graph.meta.MetaType;
import com.lightgraph.graph.modules.storage.KeyValue;
import com.lightgraph.graph.settings.GraphSetting;
import com.lightgraph.graph.utils.ByteUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GraphMeta extends ElementMeta {
    private static Log LOG = LogFactory.getLog(GraphMeta.class);
    private GraphSetting setting;
    private long version;

    public GraphMeta(byte[] bytes) {
        super(bytes);
        int pos = super.size();
        setting = new GraphSetting(ByteUtils.getBytes(bytes, pos, bytes.length - pos));
    }

    public GraphMeta(List<KeyValue> keyValues) {
        super(keyValues);
        for (KeyValue keyValue : keyValues) {
            byte[] key = keyValue.getKey();
            byte[] secKey = keyValue.getSecKey();
            byte[] value = keyValue.getValue();
            List<byte[]> keyItems = ByteUtils.split(key, GraphConstant.KEY_DELIMITER.getBytes());
            if (type == null) {
                type = MetaType.valueOf(keyItems.get(0)[0]);
                name = new String(keyItems.get(1));
                setting = new GraphSetting(name);
            }
            switch (secKey[0]) {
                case 't':
                    createTime = ByteUtils.getLong(value, 0);
                    break;
                case 'i':
                    id = ByteUtils.getLong(value, 0);
                    break;
                case 'c':
                    List<byte[]> secKeyItems = ByteUtils.split(secKey, GraphConstant.KEY_DELIMITER.getBytes());
                    String k = new String(secKeyItems.get(1));
                    String v = new String(value);
                    setting.set(k, v);
                    break;
            }
        }
    }

    public GraphMeta(String name) {
        super(name, MetaType.GRAPH);
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public GraphSetting getSetting() {
        return setting;
    }

    public void setSetting(GraphSetting setting) {
        this.setting = setting;
    }

    @Override
    public byte[] getBytes() {
        int pos = super.size();
        byte[] data = super.getBytes();
        ByteUtils.putBytes(data, pos, setting.getBytes());
        return data;
    }

    @Override
    public int size() {
        return super.size()
                + setting.size();
    }

    @Override
    public List<KeyValue> toKVS() {
        List<KeyValue> result = super.toKVS();
        byte[] key = key();
        Map<String, String> map = setting.getConfig();
        for (String sk : map.keySet()) {
            KeyValue ckv = new KeyValue(key, ByteUtils.concat(new byte[]{'c'}, GraphConstant.KEY_DELIMITER.getBytes(), sk.getBytes()), map.get(sk).getBytes());
            result.add(ckv);
        }
        return result;
    }

    public static KeyValue getKey(String name) {
        return new KeyValue(ByteUtils.concat(new byte[]{MetaType.GRAPH.getValue()}, GraphConstant.KEY_DELIMITER.getBytes(), name.getBytes()), null);
    }

    public static String getGraphName(byte[] meta) {
        return new String(ByteUtils.split(meta, GraphConstant.KEY_DELIMITER.getBytes()).get(1));
    }

    @Override
    public String toString() {
        return super.toString() + ",graphSettting:" + setting.toString();
    }

    @Override
    protected byte[] key() {
        return ByteUtils.concat(new byte[]{type.getValue()}, GraphConstant.KEY_DELIMITER.getBytes(), name.getBytes());

    }
}
