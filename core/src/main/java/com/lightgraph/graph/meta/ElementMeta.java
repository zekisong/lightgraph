package com.lightgraph.graph.meta;

import com.lightgraph.graph.constant.GraphConstant;
import com.lightgraph.graph.modules.storage.KeyValue;
import com.lightgraph.graph.utils.ByteUtils;

import java.util.ArrayList;
import java.util.List;

public abstract class ElementMeta implements Meta {
    protected MetaType type;
    protected String name;
    protected long createTime;
    protected long id;
    protected transient volatile MetaState state;

    public ElementMeta(byte[] bytes) {
        int pos = ByteUtils.RESERVED_BYTE_SIZE＿FOR_TX;
        type = MetaType.valueOf(ByteUtils.getByte(bytes, pos));
        pos = pos + 1;
        int nameSize = ByteUtils.getInt(bytes, pos);
        pos = pos + ByteUtils.SIZE_INT;
        name = ByteUtils.getString(bytes, pos, nameSize);
        pos = pos + nameSize;
        createTime = ByteUtils.getLong(bytes, pos);
        pos = pos + ByteUtils.SIZE_LONG;
        id = ByteUtils.getLong(bytes, pos);
    }

    public ElementMeta(List<KeyValue> keyValues) {
    }

    public ElementMeta(String name, MetaType type) {
        this.name = name;
        this.type = type;
    }

    public MetaState getState() {
        return state;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public void setState(MetaState state) {
        this.state = state;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    @Override
    public byte[] getBytes() {
        int size = size();
        byte[] data = new byte[size];
        int pos = ByteUtils.RESERVED_BYTE_SIZE＿FOR_TX;
        pos = ByteUtils.put(data, pos, type.getValue());
        pos = ByteUtils.putInt(data, pos, name.length());
        pos = ByteUtils.putString(data, pos, name);
        pos = ByteUtils.putLong(data, pos, createTime);
        ByteUtils.putLong(data, pos, id);
        return data;
    }

    @Override
    public int size() {
        return ByteUtils.RESERVED_BYTE_SIZE＿FOR_TX
                + ByteUtils.SIZE_BYTE
                + ByteUtils.SIZE_INT
                + name.length()
                + ByteUtils.SIZE_LONG
                + ByteUtils.SIZE_LONG;
    }

    @Override
    public String toString() {
        return "name:" + name + ",metaType:" + type + ",createTime:" + createTime + ",id:" + id;
    }

    protected abstract byte[] key();

    public List<KeyValue> toKVS() {
        List<KeyValue> result = new ArrayList<>();
        byte[] key = key();
        byte[] time = new byte[ByteUtils.SIZE_LONG];
        ByteUtils.putLong(time, 0, createTime);
        byte[] idb = new byte[ByteUtils.SIZE_LONG];
        ByteUtils.putLong(idb, 0, id);
        KeyValue timeKV = new KeyValue(key, new byte[]{'t'}, time);
        KeyValue idKV = new KeyValue(key, new byte[]{'i'}, idb);
        result.add(timeKV);
        result.add(idKV);
        return result;
    }
}
