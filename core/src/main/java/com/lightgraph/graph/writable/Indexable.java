package com.lightgraph.graph.writable;

import com.lightgraph.graph.meta.MetaType;
import com.lightgraph.graph.modules.storage.Key;
import com.lightgraph.graph.modules.storage.KeyValue;
import com.lightgraph.graph.utils.ByteUtils;
import com.lightgraph.graph.utils.SerdeUtils;
import java.util.List;

public interface Indexable {

    List<Key> indexKeys();

    static Key getIndexKey(Object... values) {
        if (values.length < 1) {
            throw new RuntimeException("index value must more than 0.");
        }
        byte[] indexKey = new byte[]{MetaType.INDEX.getValue()};
        for (Object value : values) {
            indexKey = ByteUtils.concat(indexKey, SerdeUtils.getBytes(value));
        }
        return new Key(0, indexKey);
    }
}
