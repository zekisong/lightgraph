package rocksdb;

import com.lightgraph.graph.config.Configurable;
import com.lightgraph.graph.constant.GraphConstant;
import com.lightgraph.graph.cluster.Replication;
import com.lightgraph.graph.modules.storage.BackendStorageHandler;
import com.lightgraph.graph.modules.storage.BackendStorageModule;
import com.lightgraph.graph.cluster.node.Node;
import com.lightgraph.graph.utils.ReflectUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import rocksdb.constant.RocksConstant;
import rocksdb.strategy.AllocateStorageStrategy;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class BackendStorageModuleImpl implements BackendStorageModule {

    private static final Log LOG = LogFactory.getLog(BackendStorageModuleImpl.class);
    private List<String> dataDirs;
    private AllocateStorageStrategy allocateStorageStrategy;
    private Map<String, BackendStorageHandler> openStorageMap = new ConcurrentHashMap();

    @Override
    public void setup(Configurable config) {
        dataDirs = Arrays.asList(config.get(RocksConstant.ROCKSDB_DATA_PATH, RocksConstant.ROCKSDB_DATA_PATH_DEFAULT)
                .split(GraphConstant.SPLIT_ARRAY_TOKEN));
        String clazz = config.get(RocksConstant.ROCKSDB_ALLOCATE_PATH_STRATEGY,
                RocksConstant.ROCKSDB_ALLOCATE_PATH_STRATEGY_DEFAULT);
        try {
            allocateStorageStrategy = ReflectUtils.getInstance(clazz);
        } catch (Exception e) {
            LOG.error("init allocate storage strategy tool failed!", e);
            System.exit(1);
        }
    }

    @Override
    public boolean start() {
        for (String dataDir : dataDirs) {
            File files = new File(dataDir);
            if (!files.exists()) {
                files.mkdir();
            }
        }
        return true;
    }

    @Override
    public boolean shutdown() {
        for (String dataDir : dataDirs) {
            File files = new File(dataDir);
            for (File storageDir : files.listFiles()) {
                String name = storageDir.getName();
                Replication replication = Replication.valueOf(name);
                replication.setLocation(Node.myself());
                BackendStorageHandler storage = new RocksDBStorage(dataDir, replication);
                try {
                    storage.close();
                } catch (IOException e) {
                    LOG.error(String.format("close database %s failed!", name));
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public String moduleInfo() {
        return "rocksdb impl of backendstorge module, clazz:" + this.getClass().getName();
    }

    @Override
    public BackendStorageHandler createStorageIfNotExist(Replication replication) {
        String storageName = replication.getName();
        BackendStorageHandler storage;
        if (openStorageMap.containsKey(storageName)) {
            return openStorageMap.get(storageName);
        } else {
            String path = allocateStorageStrategy.allocatePath(this, storageName);
            storage = new RocksDBStorage(path, replication);
            try {
                storage.open();
            } catch (IOException e) {
                e.printStackTrace();
            }
            openStorageMap.put(storageName, storage);
        }
        return storage;
    }

    public List<String> getDataDirs() {
        return dataDirs;
    }


    public List<Replication> getExistReplication() {
        List<Replication> replications = new ArrayList<>();
        for (String dataDir : dataDirs) {
            File files = new File(dataDir);
            for (File storageDir : files.listFiles()) {
                String name = storageDir.getName();
                Replication replication = Replication.valueOf(name);
                replication.setLocation(Node.myself());
                replications.add(replication);
            }
        }
        return replications;
    }
}
