package rocksdb;

import com.lightgraph.graph.modules.storage.Key;
import com.lightgraph.graph.modules.storage.KeyValue;
import com.lightgraph.graph.utils.ByteUtils;
import java.util.Iterator;
import java.util.function.Function;
import org.rocksdb.RocksIterator;

public class MapIterator implements Iterator<KeyValue> {

    private Function transformer;
    private RocksIterator iterator;
    private byte[] end;

    public MapIterator(RocksIterator iterator, byte[] end) {
        this.iterator = iterator;
        this.end = end;
    }

    public <R> MapIterator map(Function<KeyValue, R> function) {
        transformer = function;
        return this;
    }

    @Override
    public boolean hasNext() {
        if (!ByteUtils.startWith(iterator.key(), end)) {
            return false;
        }
        return iterator.isValid();
    }

    @Override
    public KeyValue next() {
        Key key = Key.warp(iterator.key());
        KeyValue kv = new KeyValue(key, iterator.value());
        iterator.next();
        return (KeyValue) transformer.apply(kv);
    }
}
