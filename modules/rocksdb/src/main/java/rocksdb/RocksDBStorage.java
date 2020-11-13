package rocksdb;

import com.lightgraph.graph.constant.GraphConstant;
import com.lightgraph.graph.cluster.Replication;
import com.lightgraph.graph.modules.storage.BackendStorageHandler;
import com.lightgraph.graph.modules.storage.KeyValue;
import com.lightgraph.graph.utils.ByteUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rocksdb.*;
import rocksdb.exception.BackendException;

import java.io.IOException;
import java.util.ArrayList;
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
            rocksDB.put(encodeKey(keyValue), keyValue.getValue());
        } catch (RocksDBException e) {
            throw new BackendException("set error!", e);
        }
    }

    @Override
    public void delete(byte[] k) throws IOException {

    }

    @Override
    public KeyValue get(KeyValue keyValue) throws IOException {
        try {
            byte[] v = rocksDB.get(encodeKey(keyValue));
            keyValue.setValue(v);
            return keyValue;
        } catch (RocksDBException e) {
            throw new BackendException("get error!", e);
        }
    }

    @Override
    public List<KeyValue> scan(KeyValue keyValue) {
        byte[] start = keyValue.getKey();
        ReadOptions ro = new ReadOptions();
        ro.setTotalOrderSeek(true);
        RocksIterator iterator = rocksDB.newIterator(ro);
        iterator.seek(start);
        List<KeyValue> result = new ArrayList<>();
        while (iterator.isValid()) {
            byte[] key = iterator.key();
            byte[] value = iterator.value();
            if (ByteUtils.startWith(key, start)) {
                KeyValue kv = decodeKey(key);
                kv.setValue(value);
                result.add(kv);
                iterator.next();
            } else {
                break;
            }
        }
        return result;
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

    public static byte[] encodeKey(KeyValue keyValue) {
        return ByteUtils.concat(keyValue.getKey(), GraphConstant.SEC_KEY_DELIMITER.getBytes(), keyValue.getSecKey());
    }

    public static KeyValue decodeKey(byte[] key) {
        List<byte[]> bytes = ByteUtils.split(key, GraphConstant.SEC_KEY_DELIMITER.getBytes());
        byte[] k = bytes.get(0);
        byte[] secK = bytes.size() > 1 ? bytes.get(1) : null;
        return new KeyValue(k, secK, null);
    }
}
