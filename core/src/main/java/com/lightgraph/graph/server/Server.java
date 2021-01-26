package com.lightgraph.graph.server;

import com.lightgraph.graph.config.GraphConfig;
import com.lightgraph.graph.constant.GraphConstant;
import com.lightgraph.graph.cluster.Partition;
import com.lightgraph.graph.cluster.Replication;
import com.lightgraph.graph.data.Edge;
import com.lightgraph.graph.data.Vertex;
import com.lightgraph.graph.meta.EdgeMeta;
import com.lightgraph.graph.meta.LabelMeta;
import com.lightgraph.graph.meta.LabelType;
import com.lightgraph.graph.meta.MetaType;
import com.lightgraph.graph.meta.VertexMeta;
import com.lightgraph.graph.meta.cluster.GraphMeta;
import com.lightgraph.graph.modules.*;
import com.lightgraph.graph.modules.consensus.ConsensusHandler;
import com.lightgraph.graph.modules.consensus.ConsensusModule;
import com.lightgraph.graph.modules.restful.RestfulServerModule;
import com.lightgraph.graph.modules.rpc.MetaRpcService;
import com.lightgraph.graph.modules.rpc.RpcServiceModule;
import com.lightgraph.graph.modules.rpc.StorageRpcService;
import com.lightgraph.graph.modules.storage.BackendStorageModule;
import com.lightgraph.graph.cluster.manager.*;
import com.lightgraph.graph.cluster.node.Node;
import com.lightgraph.graph.cluster.node.NodeState;
import com.lightgraph.graph.cluster.node.NodeType;
import com.lightgraph.graph.modules.storage.Batch;
import com.lightgraph.graph.modules.storage.Key;
import com.lightgraph.graph.modules.storage.KeyValue;
import com.lightgraph.graph.storage.StorageManager;
import com.lightgraph.graph.utils.ReflectUtils;
import com.lightgraph.graph.utils.RetryUtils;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
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
        String networkClass = config
                .get(GraphConstant.GRAPH_NETWORK_MODULE, GraphConstant.GRAPH_NETWORK_MODULE_DEFAULT);
        String backendStorageClass = config
                .get(GraphConstant.GRAPH_BACKENDSTORAGE_MODULE, GraphConstant.GRAPH_BACKENDSTORAGE_MODULE_DEFAULT);
        String consensusClass = config
                .get(GraphConstant.GRAPH_CONSENSUS_MODULE, GraphConstant.GRAPH_CONSENSUS_MODULE_DEFAULT);
        String rpcServiceClass = config
                .get(GraphConstant.GRAPH_RPC_SERVICE_MODULE, GraphConstant.GRAPH_RPC_SERVICE_MODULE_DEFAULT);
        String restServiceClass = config
                .get(GraphConstant.GRAPH_RESTFUL_SERVICE_MODULE, GraphConstant.GRAPH_RESTFUL_SERVICE_MODULE_DEFAULT);
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
        String masterServers = config
                .get(GraphConstant.GRAPH_MASTER_SERVERS, GraphConstant.GRAPH_MASTER_SERVERS_DEFAULT);
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
        Replication replication;
        ConsensusHandler handler;
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
            if (Node.myself().equals(master)) {
                target = replication;
            }
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
        if (Node.myself().getType() != NodeType.MASTER) {
            return false;
        }
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
            if (leader == null || leader.getName() == null) {
                continue;
            } else {
                break;
            }
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
    public Map<Replication, List<KeyValue>> getPartitionLeader(String graph, List<KeyValue> keyValues) {
        Map<Replication, List<KeyValue>> grouped = new HashMap<>();
        for (KeyValue kv : keyValues) {
            Replication r = nodeManager.getPartitionLeader(graph, kv.getKey().getRoutingBytes());
            if (grouped.containsKey(r)) {
                grouped.get(r).add(kv);
            } else {
                List<KeyValue> kvs = new ArrayList<>();
                kvs.add(kv);
                grouped.put(r, kvs);
            }
        }
        return grouped;
    }

    @Override
    public Replication getPartitionLeader(String graph, Key key) {
        return nodeManager.getPartitionLeader(graph, key.getRoutingBytes());
    }

    @Override
    public Set<Replication> getReplications(String graph, Key key) {
        return nodeManager.getReplications(graph, key.getRoutingBytes());
    }

    @Override
    public GraphMeta getGraphMeta(String graph) {
        return RetryUtils.retry(() -> {
            MetaRpcService master = getMaster();
            return master.getGraphMeta(graph);
        }, 10, 1000);
    }

    @Override
    public VertexMeta getVertexMeta(String graph, String name) {
        return RetryUtils.retry(() -> {
            MetaRpcService master = getMaster();
            return master.getVertexMeta(graph, name);
        }, 10, 1000);
    }

    @Override
    public EdgeMeta getEdgeMeta(String graph, String name) {
        return RetryUtils.retry(() -> {
            MetaRpcService master = getMaster();
            return master.getEdgeMeta(graph, name);
        }, 10, 1000);
    }

    public boolean addVertex(String graph, String label, Long id, Map<String, Object> properties) {
        VertexMeta meta = getVertexMeta(graph, label);
        Vertex vertex = new Vertex(meta);
        for (String p : properties.keySet()) {
            vertex.addProperty(p, properties.get(p));
        }
        vertex.setId(id);
        List<KeyValue> keyValues = vertex.toMutations();
        Map<Replication, List<KeyValue>> buckets = getPartitionLeader(graph, keyValues);
        for (Replication leader : buckets.keySet()) {
            StorageRpcService service = getRpcServiceHandler(leader.getLocation());
            service.batchPut(graph, leader, new Batch(buckets.get(leader)));
        }
        return true;
    }

    public boolean addEdge(String graph, String label, Long subject, Long object, Map<String, Object> properties) {
        EdgeMeta meta = getEdgeMeta(graph, label);
        Edge edge = new Edge(meta);
        edge.setSubject(subject);
        edge.setObject(object);
        for (String p : properties.keySet()) {
            edge.addProperty(p, properties.get(p));
        }
        List<KeyValue> keyValues = edge.toMutations();
        Map<Replication, List<KeyValue>> buckets = getPartitionLeader(graph, keyValues);
        for (Replication leader : buckets.keySet()) {
            StorageRpcService service = getRpcServiceHandler(leader.getLocation());
            service.batchPut(graph, leader, new Batch(buckets.get(leader)));
        }
        return true;
    }

    public boolean addEdges(String graph, String label, List<Edge> edges) {
        EdgeMeta meta = getEdgeMeta(graph, label);
        if (meta == null || meta.getId() == null) {
            throw new RuntimeException("invalid meta!");
        }
        List<KeyValue> batchKvs = new ArrayList<>();
        edges.forEach(e -> {
            e.setMeta(meta);
            batchKvs.addAll(e.toMutations());
        });
        Map<Replication, List<KeyValue>> buckets = getPartitionLeader(graph, batchKvs);
        for (Replication leader : buckets.keySet()) {
            StorageRpcService service = getRpcServiceHandler(leader.getLocation());
            service.batchPut(graph, leader, new Batch(buckets.get(leader)));
        }
        return true;
    }

    public Vertex getVertex(String graph, String label, Long id) {
        VertexMeta meta = getVertexMeta(graph, label);
        Key key = Vertex.makeKey(meta.getGraphId(), meta.getId(), id);
        Replication leader = getPartitionLeader(graph, key);
        StorageRpcService service = getRpcServiceHandler(leader.getLocation());
        KeyValue result = service.get(graph, key);
        Vertex vertex = Vertex.getInstance(result.getValue(), Vertex.class);
        vertex.setMeta(meta);
        return vertex;
    }

    public Edge getEdge(String graph, String label, Long subject, Long object) {
        EdgeMeta meta = getEdgeMeta(graph, label);
        Key key = Edge.makeKey(meta.getGraphId(), subject, meta.getId(), object);
        Replication leader = getPartitionLeader(graph, key);
        StorageRpcService service = getRpcServiceHandler(leader.getLocation());
        KeyValue result = service.get(graph, key);
        Edge edge = Edge.getInstance(result.getValue(), Edge.class);
        edge.setMeta(meta);
        return edge;
    }

    public List<Edge> outEdges(String graph, String label, Long subject) {
        EdgeMeta meta = getEdgeMeta(graph, label);
        Key key = Edge.makeLabelScanKey(meta.getGraphId(), subject, meta.getId());
        Replication leader = getPartitionLeader(graph, key);
        StorageRpcService service = getRpcServiceHandler(leader.getLocation());
        Iterator<KeyValue> result = service.scan(graph, key);
        List<Edge> edges = new ArrayList<>();
        while (result.hasNext()) {
            Edge edge = Edge.getInstance(result.next().getValue(), Edge.class);
            edge.setMeta(meta);
            edges.add(edge);
        }
        return edges;
    }

    public List<Edge> outEdges(String graph, Long subject, int step) {
        List<Edge> result = new ArrayList<>();
        Set<Long> distinctSet = new HashSet<>();
        Map<Long, EdgeMeta> metaCache = new HashMap<>();
        GraphMeta graphMeta = getGraphMeta(graph);
        nextEdges(graphMeta, subject, step, distinctSet, metaCache, (e) -> {
            result.add(e);
            return null;
        });
        return result;
    }

    public long coutEdges(String graph, Long subject, int step) {
        List<Edge> result = new ArrayList<>();
        Set<Long> distinctSet = new HashSet<>();
        Map<Long, EdgeMeta> metaCache = new HashMap<>();
        GraphMeta graphMeta = getGraphMeta(graph);
        AtomicLong count = new AtomicLong();
        nextEdges(graphMeta, subject, step, distinctSet, metaCache, (e) -> {
            count.incrementAndGet();
            return null;
        });
        return count.get();
    }

    public void nextEdges(GraphMeta graphMeta, Long subject, int step, Set<Long> distinctSet,
            Map<Long, EdgeMeta> metaCache, Function<Edge, Object> function) {
        if (step == 0) {
            return;
        }
        Key key = Edge.makeSubjectScanKey(graphMeta.getId(), subject);
        Replication leader = getPartitionLeader(graphMeta.getName(), key);
        StorageRpcService service = getRpcServiceHandler(leader.getLocation());
        Iterator<KeyValue> result = service.scan(graphMeta.getName(), key);
        while (result.hasNext()) {
            KeyValue kv = result.next();
            Long eid = Edge.getEdgeMetaIdFromKeyValue(kv);
            EdgeMeta meta;
            if (metaCache.containsKey(eid)) {
                meta = metaCache.get(eid);
            } else {
                MetaRpcService master = getMaster();
                meta = (EdgeMeta) master.getLabelMetaById(eid, LabelType.EDGE);
                metaCache.put(eid, meta);
            }
            Edge edge = Edge.getInstance(kv.getValue(), Edge.class);
            if (distinctSet.contains(edge.getObject())) {
                continue;
            }
            distinctSet.add(edge.getObject());
            nextEdges(graphMeta, edge.getObject(), step - 1, distinctSet, metaCache, function);
            edge.setMeta(meta);
            function.apply(edge);
        }
    }

    public List<Long> outEdgeIds(String graph, String label, Long subject) {
        List<Edge> outEdges = outEdges(graph, label, subject);
        return outEdges.stream().map(e -> e.getObject()).collect(Collectors.toList());
    }

    @Override
    public List<GraphMeta> listGraphMeta() {
        MetaRpcService master = getMaster();
        return master.listGraphMeta();
    }

    @Override
    public List<LabelMeta> listLabelMeta(String graph, MetaType metaType) {
        MetaRpcService master = getMaster();
        return master.listLabelMeta(graph, metaType);
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