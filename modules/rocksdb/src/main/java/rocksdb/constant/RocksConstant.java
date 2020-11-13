package rocksdb.constant;

import com.lightgraph.graph.constant.GraphConstant;

public class RocksConstant {
    public static final String ROCKSDB_DATA_PATH = "rocksdb.data.path";
    public static final String ROCKSDB_DATA_PATH_DEFAULT = GraphConstant.GRAPH_HOME + "/data";

    public static final String ROCKSDB_ALLOCATE_PATH_STRATEGY = "rocksdb.allocate.path.strategy";
    public static final String ROCKSDB_ALLOCATE_PATH_STRATEGY_DEFAULT = "rocksdb.strategy.HashAllocateStorageStrategy";

}