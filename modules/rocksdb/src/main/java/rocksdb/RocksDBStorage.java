package rocksdb;

import com.lightgraph.graph.constant.GraphConstant;
import com.lightgraph.graph.cluster.Replication;
import com.lightgraph.graph.modules.storage.BackendStorageHandler;
import com.lightgraph.graph.modules.storage.Key;
import com.lightgraph.graph.modules.storage.KeyValue;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rocksdb.*;
import rocksdb.exception.BackendException;

import java.io.IOException;
import java.util.List;

public class RocksDBStorage implements BackendStorageHandler {

    private static final Log LOG = LogFactory.getLog(BackendStorageModuleImpl.class);
    private String dataDir;
    private String name;
    private RocksDB rocksDB;

    static {
        RocksDB.loadLibrary();
    }

    public RocksDBStorage(String dataDir, Replication replication) {
        this.dataDir = dataDir;
        this.name = replication.getName();
    }

    @Override
    public void open() {
        try {
            if (rocksDB == null) {
                synchronized (RocksDBStorage.class) {
                    if (rocksDB == null) {
                        rocksDB = RocksDB.open(dataDir + GraphConstant.DIR_SEPARATOR + name);
                    }
                }
            } else {
                LOG.warn(String.format("storage %s in %s already opend!", name, dataDir));
            }
        } catch (Exception e) {
            LOG.warn(String.format("storage %s in %s open failed! skip open ...", name, dataDir), e);
        }
    }

    @Override
    public void close() {
        rocksDB.close();
    }

    @Override
    public void set(KeyValue keyValue) throws BackendException {
        try {
            rocksDB.put(keyValue.getKey().bytes(), keyValue.getValue());
        } catch (RocksDBException e) {
            throw new BackendException("set error!", e);
        }
    }

    @Override
    public void batchSet(List<KeyValue> keyValues) throws IOException {
        WriteBatch batch = new WriteBatch();
        try {
            for (KeyValue kv : keyValues) {
                batch.put(kv.getKey().bytes(), kv.getValue());
            }
            rocksDB.write(new WriteOptions(), batch);
        } catch (RocksDBException e) {
            throw new BackendException("batch set error!", e);
        }
    }

    @Override
    public void delete(byte[] k) {

    }

    @Override
    public KeyValue get(Key key) throws IOException {
        try {
            byte[] v = rocksDB.get(key.bytes());
            return new KeyValue(key, v);
        } catch (RocksDBException e) {
            throw new BackendException("get error!", e);
        }
    }

    @Override
    public Iterator<KeyValue> scan(Key start) {
        ReadOptions ro = new ReadOptions();
        ro.setTotalOrderSeek(true);
        RocksIterator iterator = rocksDB.newIterator(ro);
        iterator.seek(start.bytes());
        return new MapIterator(iterator, start.bytes()).map(kv -> kv);
    }

    @Override
    public List<KeyValue> multiGet(List<byte[]> k) throws IOException {
        try {
            rocksDB.multiGet(k);
        } catch (RocksDBException e) {
            throw new BackendException("multi get error!", e);
        }
        return null;
    }
}
