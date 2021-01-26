package com.lightgraph.graph.meta;

import com.lightgraph.graph.modules.storage.Key;
import com.lightgraph.graph.modules.storage.KeyValue;

import com.lightgraph.graph.writable.Writable;
import java.util.ArrayList;
import java.util.List;

public abstract class ElementMeta extends Writable implements Meta {

    protected MetaType type;
    protected String name;
    protected Long createTime;
    protected Long id;
    protected transient volatile MetaState state;

    public ElementMeta() {
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

    public Long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Long createTime) {
        this.createTime = createTime;
    }

    public void setState(MetaState state) {
        this.state = state;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "name:" + name + ",metaType:" + type + ",createTime:" + createTime + ",id:" + id;
    }

    protected abstract Key key();

    public static Key getIndexKey(Object... values) {
        if (values.length < 1) {
            throw new RuntimeException("index value must more than 0.");
        }
        Long id = (Long) values[0];
        return new Key(MetaType.INDEX.getValue(), id);
    }

    @Override
    public List<Key> indexKeys() {
        List<Key> result = new ArrayList<>();
        result.add(getIndexKey(id));
        return result;
    }

    @Override
    public List<KeyValue> toMutations() {
        Key key = key();
        List<KeyValue> mutation = new ArrayList<>();
        mutation.add(new KeyValue(key, getBytes()));
        List<Key> indexkeys = indexKeys();
        if (indexkeys != null) {
            for (Key indexkey : indexkeys) {
                mutation.add(new KeyValue(indexkey, key.bytes()));
            }
        }
        return mutation;
    }

    public static <T> T getInstance(KeyValue keyValue, Class<T> clazz) {
        return getInstance(keyValue.getValue(), clazz);
    }
}