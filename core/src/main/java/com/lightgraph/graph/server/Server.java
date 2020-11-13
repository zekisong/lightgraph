package com.lightgraph.graph.server;

import com.lightgraph.graph.config.GraphConfig;
import com.lightgraph.graph.constant.GraphConstant;
import com.lightgraph.graph.cluster.Partition;
import com.lightgraph.graph.cluster.Replication;
import com.lightgraph.graph.data.Edge;
import com.lightgraph.graph.data.Vertex;
import com.lightgraph.graph.meta.EdgeMeta;
import com.lightgraph.graph.meta.VertexMeta;
import com.lightgraph.graph.meta.cluster.GraphMeta;
import com.lightgraph.graph.modules.*;
import com.lightgraph.graph.modules.consensus.ConsensusHandler;
import com.lightgraph.graph.modules.consensus.ConsensusModule;
import com.lightgraph.graph.modules.restful.RestfulServerModule;
import com.lightgraph.graph.modules.rpc.StorageRpcService;
import com.lightgraph.graph.modules.rpc.MetaRpcService;
import com.lightgraph.graph.modules.rpc.RpcServiceModule;
import com.lightgraph.graph.modules.storage.BackendStorageModule;
import com.lightgraph.graph.cluster.manager.*;
import com.lightgraph.graph.cluster.node.Node;
import com.lightgraph.graph.cluster.node.NodeState;
import com.lightgraph.graph.cluster.node.NodeType;
import com.lightgraph.graph.modules.storage.KeyValue;
import com.lightgraph.graph.storage.StorageManager;
import com.lightgraph.graph.utils.ReflectUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

public class Server implements ServerService {
    private static Log LOG = LogFactory.getLog(Server.class);
    private static Server INSTANCE;
    private GraphConfig config = GraphConfig.getInstance();
    private NetworkModule networkModule;
    private ConsensusModule consensusModule;
    private BackendStorageModule backendStorageModule;
    private RpcServiceModule rpcServiceModule;
    private RestfulServerModule restServiceModule;
    private NodeManager nodeManager;
    private StorageManager storageManager;
    private List<Node> masters;

    public void init() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        storageManager = new StorageManager(this);
        buildMasterNodes();
        initNodeManager();
        String networkClass = config.get(GraphConstant.GRAPH_NETWORK_MODULE, GraphConstant.GRAPH_NETWORK_MODULE_DEFAULT);
        String backendStorageClass = config.get(GraphConstant.GRAPH_BACKENDSTORAGE_MODULE, GraphConstant.GRAPH_BACKENDSTORAGE_MODULE_DEFAULT);
        String consensusClass = config.get(GraphConstant.GRAPH_CONSENSUS_MODULE, GraphConstant.GRAPH_CONSENSUS_MODULE_DEFAULT);
        String rpcServiceClass = config.get(GraphConstant.GRAPH_RPC_SERVICE_MODULE, GraphConstant.GRAPH_RPC_SERVICE_MODULE_DEFAULT);
        String restServiceClass = config.get(GraphConstant.GRAPH_RESTFUL_SERVICE_MODULE, GraphConstant.GRAPH_RESTFUL_SERVICE_MODULE_DEFAULT);
        networkModule = ReflectUtils.getInstance(networkClass);
        consensusModule = ReflectUtils.getInstance(consensusClass);
        backendStorageModule = ReflectUtils.getInstance(backendStorageClass);
        rpcServiceModule = ReflectUtils.getInstance(rpcServiceClass);
        restServiceModule = ReflectUtils.getInstance(restServiceClass);

        backendStorageModule.setup(config);
        networkModule.setup(config);
        consensusModule.setup(config);
        rpcServiceModule.setup(config);
        restServiceModule.setup(config);
        consensusModule.bindNetwork(networkModule);
        rpcServiceModule.bindNetwork(networkModule);
        rpcServiceModule.bindRpcService(nodeManager, storageManager);

        backendStorageModule.start();
        networkModule.start();
        consensusModule.start();
        rpcServiceModule.start();
        restServiceModule.start();

        storageManager.bindBackendStorageModule(backendStorageModule);
        storageManager.bindConsensusModule(consensusModule);
        Node.myself().setState(NodeState.HEALTH);

        startNode();
        storageManager.reportStorage();
        LOG.info("server start finished!");
    }

    public void buildMasterNodes() {
        masters = new ArrayList<>();
        String masterServers = config.get(GraphConstant.GRAPH_MASTER_SERVERS, GraphConstant.GRAPH_MASTER_SERVERS_DEFAULT);
        String[] masterServersArray = masterServers.split(GraphConstant.SPLIT_ARRAY_TOKEN);
        for (String name : masterServersArray) {
            String addr = name.split(GraphConstant.SPLIT_NET_ADDRESS_TOKEN)[0];
            Short port = Short.valueOf(name.split(GraphConstant.SPLIT_NET_ADDRESS_TOKEN)[1]);
            Node master = new Node(name, addr, port);
            masters.add(master);
        }
    }

    public void initNodeManager() {
        if (imMaster()) {
            this.nodeManager = new MasterNodeManager(storageManager, this, masters);
        } else {
            this.nodeManager = new DataNodeManager(storageManager, this);
        }
    }

    public void startNode() {
        Replication replication = null;
        ConsensusHandler handler = null;
        if (imMaster()) {
            MasterNodeManager manager = (MasterNodeManager) nodeManager;
            replication = buildMasterInstance();
            handler = storageManager.loadConsensusInstance(replication, true, manager);
            manager.setConsensusHandler(handler);
        }
        blockUntilJoinToMaster();
    }

    public Replication buildMasterInstance() {
        Replication target = null;
        Partition partition = new Partition(GraphConstant.MASTER_GROUP_NAME, 0);
        Map<Integer, Replication> replications = new HashMap<>();
        for (int replicationIndex = 0; replicationIndex < masters.size(); replicationIndex++) {
            Node master = masters.get(replicationIndex);
            Replication replication = new Replication(partition, replicationIndex, master);
            replication.setGroupSize(masters.size());
            replications.put(replicationIndex, replication);
            if (Node.myself().equals(master))
                target = replication;
        }
        partition.setReplications(replications);
        return target;
    }

    public void blockUntilJoinToMaster() {
        while (true) {
            try {
                //STEP1 find master leader
                Node leader = findLeader();
                if (leader == null || leader.getName() == null) {
                    LOG.info("master not found! retry after 1s.");
                    Thread.sleep(1000);
                    continue;
                }
                LOG.info(String.format("found master leader:%s", leader));

                //STEP2 add local node to cluster
                if (imMasterLeader()) {
                    nodeManager.joinToCluster(Node.myself());
                    return;
                }

                //STEP3 join to cluster
                MetaRpcService service = getRpcServiceHandler(leader);
                boolean ret = service.joinToCluster(Node.myself());
                if (!ret) {
                    LOG.info("join to cluster failed! retry after 1s.");
                    Thread.sleep(1000);
                    continue;
                }
                LOG.info(String.format("joined master leader:%s", leader));
                break;
            } catch (Exception e) {
                LOG.warn("join to cluster failed! retry after 1s ...", e);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean imMaster() {
        if (masters.indexOf(Node.myself()) >= 0) {
            return true;
        } else {
            return false;
        }
    }

    public boolean imMasterLeader() {
        if (Node.myself().getType() != NodeType.MASTER)
            return false;
        Node leader = nodeManager.getLeader();
        if (Node.myself().equals(leader)) {
            return true;
        } else {
            return false;
        }
    }

    public Node findLeader() {
        Node leader = null;
        for (Node master : masters) {
            if (Node.myself().equals(master)) {
                continue;
            }
            MetaRpcService service = getRpcServiceHandler(master);
            leader = service.getLeader();
            if (leader == null || leader.getName() == null)
                continue;
            else
                break;
        }
        return leader;
    }

    private <T> T getRpcServiceHandler(Node node) {
        return rpcServiceModule.getRpcServiceHandler(node);
    }

    @Override
    public MetaRpcService getMasterLeader() {
        waitMasterLeaderSync();
        return rpcServiceModule.getRpcServiceHandler(nodeManager.getLeader());
    }

    @Override
    public MetaRpcService getMaster() {
        Node random = masters.get((int) (Math.random() * masters.size()));
        return rpcServiceModule.getRpcServiceHandler(random);
    }

    @Override
    public boolean put(String graph, KeyValue keyValue) {
        Replication leader = getPartitionLeader(graph, keyValue);
        StorageRpcService service = getRpcServiceHandler(leader.getLocation());
        boolean ret = service.put(graph, keyValue);
        return ret;
    }

    @Override
    public List<KeyValue> scan(String graph, KeyValue keyValue) {
        Replication leader = getPartitionLeader(graph, keyValue);
        StorageRpcService service = getRpcServiceHandler(leader.getLocation());
        return service.scan(graph, keyValue);
    }

    @Override
    public KeyValue get(String graph, KeyValue keyValue) {
        Replication leader = getPartitionLeader(graph, keyValue);
        StorageRpcService service = getRpcServiceHandler(leader.getLocation());
        return service.get(graph, keyValue);
    }

    @Override
    public Replication getPartitionLeader(String graph, KeyValue keyValue) {
        return nodeManager.getPartitionLeader(graph, keyValue.getKey());
    }

    @Override
    public Set<Replication> getReplications(String graph, KeyValue keyValue) {
        return nodeManager.getReplications(graph, keyValue.getKey());
    }

    @Override
    public GraphMeta getGraphMeta(String graph) {
        MetaRpcService master = getMaster();
        return master.getGraphMeta(graph);
    }

    @Override
    public VertexMeta getVertexMeta(String graph, String name) {
        MetaRpcService master = getMaster();
        return master.getVertexMeta(graph, name);
    }

    @Override
    public EdgeMeta getEdgeMeta(String graph, String name) {
        MetaRpcService master = getMaster();
        return master.getEdgeMeta(graph, name);
    }

    public Vertex addVertex(String graph, String label, Map<String, Object> properties) {
        return null;
    }

    public Edge addEdge(String graph, String src, String dest, String label, Map<String, Object> properties) {
        return null;
    }

    @Override
    public List<GraphMeta> listGraphMeta() {
        MetaRpcService master = getMaster();
        return master.listGraphMeta();
    }

    public void waitMasterLeaderSync() {
        while (true) {
            if (nodeManager == null || nodeManager.getLeader() == null) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                break;
            }
        }
    }

    public static Server getInstance() {
        if (INSTANCE == null) {
            synchronized (Server.class) {
                if (INSTANCE == null) {
                    INSTANCE = new Server();
                    try {
                        INSTANCE.init();
                    } catch (Exception e) {
                        LOG.error("server start failed!", e);
                        System.exit(1);
                    }
                }
            }
        }
        return INSTANCE;
    }

    public NodeManager getManager() {
        return nodeManager;
    }

    public static void main(String[] args) {
        Server.getInstance();
    }
}