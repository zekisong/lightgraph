package com.lightgraph.graph.cluster.manager;

import com.lightgraph.graph.command.MetaCommandRunner;
import com.lightgraph.graph.constant.GraphConstant;
import com.lightgraph.graph.exception.GraphException;
import com.lightgraph.graph.graph.EdgeMetaInfo;
import com.lightgraph.graph.graph.PropertyMetaInfo;
import com.lightgraph.graph.graph.VertexMetaInfo;
import com.lightgraph.graph.meta.EdgeMeta;
import com.lightgraph.graph.meta.LabelMeta;
import com.lightgraph.graph.meta.LabelType;
import com.lightgraph.graph.meta.MetaManager;
import com.lightgraph.graph.meta.MetaType;
import com.lightgraph.graph.meta.PropertyMeta;
import com.lightgraph.graph.meta.VertexMeta;
import com.lightgraph.graph.meta.cluster.GraphMeta;
import com.lightgraph.graph.cluster.Partition;
import com.lightgraph.graph.cluster.Replication;
import com.lightgraph.graph.modules.consensus.*;
import com.lightgraph.graph.modules.rpc.ServiceType;
import com.lightgraph.graph.modules.storage.BackendStorageHandler;
import com.lightgraph.graph.server.Server;
import com.lightgraph.graph.command.CommandObserver;
import com.lightgraph.graph.command.MasterOperation;
import com.lightgraph.graph.cluster.node.Node;
import com.lightgraph.graph.cluster.node.NodeType;
import com.lightgraph.graph.settings.GraphSetting;
import com.lightgraph.graph.storage.StorageManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 动态集群信息维护
 */
public class MasterNodeManager extends NodeManager implements LeaderChangeListener {

    private static Log LOG = LogFactory.getLog(MasterNodeManager.class);
    private ConsensusInstance instance;
    private ConsensusHandler consensusHandler;
    private Map<Node, Long> versionMap = new ConcurrentHashMap<>();
    private Object writeMutex = new Object();
    private MetaManager metaManager;
    private BackendStorageHandler metaStorage;
    private List<Node> masters;

    public MasterNodeManager(StorageManager storage, Server server, List<Node> masters) {
        super(storage, server);
        this.masters = masters;
    }

    @Override
    public void onLeaderChangeEvent(long term, ConsensusInstance leader) {
        if (instance.equals(leader)) {
            synchronized (writeMutex) {
                setLeader(leader.getLocation());
            }
        }
    }

    @Override
    public void bindInstance(ConsensusInstance instance) {
        this.instance = instance;
        this.metaStorage = storage.getStorageHandler((Replication) (instance));
        this.metaManager = new MetaManager(this.metaStorage);
    }

    @Override
    public List<byte[]> updateContext(long version) {
        long currentVersion = context.getVersion();
        List<byte[]> patchs = new ArrayList<>();
        if (version < currentVersion) {
            LOG.info(String.format("send version[%d,%d)", version, currentVersion));
            synchronized (deltas) {
                patchs = new ArrayList<>(deltas.subList((int) (version), (int) (currentVersion)));
            }
        }
        return patchs;
    }

    @Override
    public boolean addReplications(List<Replication> replications) {
        for (Replication replication : replications) {
            addReplication(replication);
        }
        return true;
    }

    @Override
    public ServiceType getServiceType() {
        return ServiceType.META;
    }

    @Override
    public ConsensusIO getConsensusIO() {
        return consensusHandler;
    }

    @Override
    public boolean createGraph(GraphSetting setting) {
        if (context.getRoutting().containsKey(setting.getGraphName())) {
            throw new GraphException("graph already exist!");
        }
        byte[] data = attachOperation(setting.getBytes(), MasterOperation.CREATE_GRAPH);
        commandRunner.writeCommand(data);
        return true;
    }

    @Override
    public boolean createGraphInner(GraphSetting setting) {
        String name = setting.getGraphName();
        int partitionCount = setting
                .get(GraphSetting.GRAPH_PARTITION_COUNT, GraphSetting.GRAPH_PARTITION_COUNT_DEFAULT);
        int replicationCount = setting
                .get(GraphSetting.GRAPH_PEPLICATION_COUNT, GraphSetting.GRAPH_PEPLICATION_COUNT_DEFAULT);
        GraphMeta meta = new GraphMeta(name);
        meta.setSetting(setting);
        try {
            metaManager.saveGraphMeta(meta);
            initRouttingTable(name, partitionCount, replicationCount);
        } catch (Exception e) {
            LOG.info("create graph failed!", e);
            return false;
        }
        return false;
    }

    @Override
    public boolean joinToCluster(Node node) {
        if (consensusHandler != null && consensusHandler.isLeader()) {
            byte[] data = attachOperation(node.getBytes(), MasterOperation.ADD_NODE);
            commandRunner.writeCommand(data);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean joinToClusterInner(Node node) {
        synchronized (writeMutex) {
            context.getNodes().put(node.getName(), node);
            versionMap.remove(node);
            this.context.incrVersion();
        }
        return true;
    }

    public void setLeader(Node leader) {
        byte[] data = attachOperation(leader.getBytes(), MasterOperation.SET_MASTER_LEADER);
        commandRunner.writeCommand(data);
    }

    @Override
    public void setLeaderInner(Node leader) {
        synchronized (writeMutex) {
            versionMap.clear();
            context.setLeader(leader);
            this.context.incrVersion();
            CompletableFuture.supplyAsync(() -> {
                while (true) {
                    try {
                        Map<Integer, Replication> metaReplications = this.context.getRoutting()
                                .get(GraphConstant.META_TABLE_NAME).get(0).getReplications();
                        if (metaReplications.size() != masters.size()) {
                            Thread.sleep(1000);
                            continue;
                        }
                        for (Replication replication : metaReplications.values()) {
                            if (replication.getLocation().equals(leader)) {
                                replication.setState(ConsensusInstanceState.LEADER);
                            } else {
                                replication.setState(ConsensusInstanceState.FOLLOWER);
                            }
                            updateReplication(replication);
                        }
                        break;
                    } catch (Exception e) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException interruptedException) {
                            interruptedException.printStackTrace();
                        }
                    }
                }
                return null;
            });
        }
    }

    @Override
    public boolean updateReplication(Replication replication) {
        byte[] data = attachOperation(replication.getBytes(), MasterOperation.UPDATE_REPLICATION);
        LOG.info("write update log:" + replication.getDescription() + "\t state:" + replication.getState());
        commandRunner.writeCommand(data);
        return true;
    }

    @Override
    public LabelMeta getLabelMetaById(Long id, LabelType type) {
        return metaManager.getLabelMetaById(id, type);
    }

    @Override
    public GraphMeta getGraphMeta(String graph) {
        return metaManager.getGraphMeta(graph);
    }

    @Override
    public List<GraphMeta> listGraphMeta() {
        return metaManager.listGraphMeta();
    }

    @Override
    public List<LabelMeta> listLabelMeta(String graph, MetaType metaType) {
        return metaManager.listLabelMeta(graph, metaType);
    }

    @Override
    public boolean addVertexMeta(VertexMetaInfo vertexMetaInfo) {
        try {
            VertexMeta meta = getVertexMeta(vertexMetaInfo.getGraph(), vertexMetaInfo.getName());
            if (meta != null) {
                throw new GraphException(String.format("vertex meta:%s already exist!", meta.getName()));
            }
            byte[] data = attachOperation(vertexMetaInfo.getBytes(), MasterOperation.ADD_VERTEX_META);
            LOG.info("add vertex meta log:" + vertexMetaInfo.getGraph() + "\t" + vertexMetaInfo.getName());
            commandRunner.writeCommand(data);
            return true;
        } catch (Exception e) {
            throw new GraphException("add vertex meta failed!", e);
        }
    }

    @Override
    public boolean addEdgeMeta(EdgeMetaInfo edgeMetaInfo) {
        try {
            EdgeMeta meta = getEdgeMeta(edgeMetaInfo.getGraph(), edgeMetaInfo.getName());
            if (meta != null) {
                throw new GraphException(String.format("edge meta:%s already exist!", meta.getName()));
            }
            byte[] data = attachOperation(edgeMetaInfo.getBytes(), MasterOperation.ADD_EDGE_META);
            LOG.info("add edge meta log:" + edgeMetaInfo.getGraph() + "\t" + edgeMetaInfo.getName());
            commandRunner.writeCommand(data);
            return true;
        } catch (Exception e) {
            throw new GraphException("add edge meta failed!", e);
        }
    }

    @Override
    public boolean addEdgeMetaInner(EdgeMetaInfo edgeMetaInfo) {
        String graph = edgeMetaInfo.getGraph();
        long graphId = getGraphMeta(graph).getId();
        EdgeMeta em = new EdgeMeta(edgeMetaInfo.getName());
        em.setGraphId(graphId);
        String subject = edgeMetaInfo.getSubject();
        String object = edgeMetaInfo.getObject();
        VertexMeta sMeta = metaManager.getVertexMeta(graphId, subject);
        VertexMeta oMeta = metaManager.getVertexMeta(graphId, object);
        em.setSubject(sMeta.getId());
        em.setObject(oMeta.getId());
        List<PropertyMeta> pms = new ArrayList<>();
        try {
            for (PropertyMetaInfo pmi : edgeMetaInfo.getProperties()) {
                PropertyMeta pm = new PropertyMeta(pmi.getName());
                pm.setDataType(pmi.getDataType());
                pm.setDefaultValue(pmi.getDefaultValue());
                pms.add(pm);
            }
            em.setProperties(pms);
            metaManager.saveLabelMeta(em);
        } catch (Exception e) {
            LOG.error("add vertex meta failed!", e);
            return false;
        }
        return true;
    }

    @Override
    public VertexMeta getVertexMeta(String graph, String name) {
        Long graphId = getGraphMeta(graph).getId();
        return metaManager.getVertexMeta(graphId, name);
    }

    @Override
    public EdgeMeta getEdgeMeta(String graph, String name) {
        Long graphId = getGraphMeta(graph).getId();
        return metaManager.getEdgeMeta(graphId, name);
    }

    @Override
    public boolean addVertexMetaInner(VertexMetaInfo vertexMetaInfo) {
        String graph = vertexMetaInfo.getGraph();
        Long graphId = getGraphMeta(graph).getId();
        VertexMeta vm = new VertexMeta(vertexMetaInfo.getName());
        vm.setGraphId(graphId);
        vm.setPK(vertexMetaInfo.getKey());
        List<PropertyMeta> pms = new ArrayList<>();
        try {
            for (PropertyMetaInfo pmi : vertexMetaInfo.getProperties()) {
                PropertyMeta pm = new PropertyMeta(pmi.getName());
                pm.setDataType(pmi.getDataType());
                pm.setDefaultValue(pmi.getDefaultValue());
                pms.add(pm);
            }
            vm.setProperties(pms);
            metaManager.saveLabelMeta(vm);
        } catch (Exception e) {
            LOG.error("add vertex meta failed!", e);
            return false;
        }
        return true;
    }

    @Override
    public boolean updateReplicationInner(Replication replication) {
        synchronized (context) {
            context.updateReplication(replication);
            context.incrVersion();
        }
        return true;
    }

    public boolean addReplication(Replication replication) {
        if (!replication.isReady()) {
            while (true) {
                try {
                    if (GraphConstant.META_TABLE_NAME.equals(replication.getGraphName())) {
                        replication.setGroupSize(masters.size());
                    } else {
                        int groupSize = getGraphMeta(replication.getGraphName()).getSetting()
                                .get(GraphSetting.GRAPH_PEPLICATION_COUNT,
                                        GraphSetting.GRAPH_PEPLICATION_COUNT_DEFAULT);
                        replication.setGroupSize(groupSize);
                    }
                    break;
                } catch (Throwable t) {
                    LOG.warn("meta not prepare!", t);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        byte[] data = attachOperation(replication.getBytes(), MasterOperation.ADD_REPLICATION);
        commandRunner.writeCommand(data);
        return true;
    }

    @Override
    public boolean addReplicationInner(Replication replication) {
        synchronized (writeMutex) {
            context.addReplication(replication);
            context.incrVersion();
        }
        return true;
    }

    public static byte[] attachOperation(byte[] data, MasterOperation operation) {
        if (data.length < 1) {
            throw new GraphException("command invalid!");
        } else {
            byte[] attactedBytes = new byte[data.length + 1];
            System.arraycopy(data, 0, attactedBytes, 1, data.length);
            attactedBytes[0] = operation.getOP();
            return attactedBytes;
        }
    }

    public void setConsensusHandler(ConsensusHandler consensusHandler) {
        this.consensusHandler = consensusHandler;
        commandRunner = new MetaCommandRunner(this, new CommandObserver() {
            @Override
            public void preRun() {

            }

            @Override
            public void afterRun(byte[] data) {
                synchronized (deltas) {
                    if (MasterOperation.shouldSync(MasterOperation.valueOf(data[0]))) {
                        deltas.add(data);
                    }
                }
            }
        });
        commandRunner.setMetaStorage(metaStorage);
        Thread thread = new Thread(commandRunner);
        thread.setDaemon(true);
        thread.setName("master-command-runner");
        thread.start();
    }

    public void initRouttingTable(String name, int partitionCount, int replicationCount) {
        List<Node> datas = new ArrayList(Arrays.asList(context.getNodes().values().toArray()));
        datas.removeIf(n -> n.getType() == NodeType.MASTER);
        for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
            Map<Integer, Replication> replications = new ConcurrentHashMap<>();
            Partition partition = new Partition(name, partitionIndex);
            for (int replicationIndex = 0; replicationIndex < replicationCount; replicationIndex++) {
                Node target = datas.get((partitionIndex * replicationCount + replicationIndex) % datas.size());
                Replication replication = new Replication(partition, replicationIndex, target);
                addReplication(replication);
                replications.put(replicationIndex, replication);
            }
            partition.setReplications(replications);
        }
    }
}

