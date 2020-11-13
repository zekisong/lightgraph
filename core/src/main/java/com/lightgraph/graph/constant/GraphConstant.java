package com.lightgraph.graph.constant;

public class GraphConstant {

    public static final String GRAPH_HOME = System.getProperty("graph_home");

    public static final String GRAPH_NETWORK_MODULE = "graph.network.module";
    public static final String GRAPH_NETWORK_MODULE_DEFAULT = "network.NetworkModuleImpl";

    public static final String GRAPH_CONSENSUS_MODULE = "graph.consensus.module";
    public static final String GRAPH_CONSENSUS_MODULE_DEFAULT = "raft.ConsensusModuleImpl";

    public static final String GRAPH_BACKENDSTORAGE_MODULE = "graph.backendstorage.module";
    public static final String GRAPH_BACKENDSTORAGE_MODULE_DEFAULT = "rocksdb.BackendStorageModuleImpl";

    public static final String GRAPH_RPC_SERVICE_MODULE = "graph.rpc.service.module";
    public static final String GRAPH_RPC_SERVICE_MODULE_DEFAULT = "rpc.RpcServiceModuleImpl";

    public static final String GRAPH_RESTFUL_SERVICE_MODULE = "graph.restful.service.module";
    public static final String GRAPH_RESTFUL_SERVICE_MODULE_DEFAULT = "restful.RestfulServerModuleImpl";

    public static final String SERVER_ADDR_PREFIX = "server.addr.prefix";
    public static final String SERVER_ADDR_PREFIX_DEFAULT = "127.0.0.1";

    public static final String SERVER_PORT_NUM = "server.port.num";
    public static final short SERVER_PORT_NUM_DEFAULT = 8899;

    public static final String GRAPH_MASTER_SERVERS = "graph.master.servers";
    public static final String GRAPH_MASTER_SERVERS_DEFAULT = SERVER_ADDR_PREFIX_DEFAULT + ":" + SERVER_PORT_NUM_DEFAULT;

    public static final String META_TABLE_NAME = "META";
    public static final String META_ID_KEY = "id";
    public static final String MASTER_GROUP_NAME = "META";
    public static final String DIR_SEPARATOR = "/";
    public static final String SPLIT_ARRAY_TOKEN = ",";
    public static final String SPLIT_NET_ADDRESS_TOKEN = ":";
    public static final String FILE_SUFFIX_TOKEN = ".";
    public static final String SPLIT_FILE_SUFFIX_TOKEN = "\\.";

    public static final String KEY_DELIMITER = "%";
    public static final String SEC_KEY_DELIMITER = "#";
}
