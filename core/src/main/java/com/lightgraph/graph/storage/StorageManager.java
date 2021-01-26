package com.lightgraph.graph.storage;

import com.lightgraph.graph.cluster.Replication;
import com.lightgraph.graph.command.StorageCommandRunner;
import com.lightgraph.graph.command.StorageOperation;
import com.lightgraph.graph.constant.GraphConstant;
import com.lightgraph.graph.exception.ReplicationNotExistException;
import com.lightgraph.graph.modules.consensus.*;
import com.lightgraph.graph.modules.rpc.MetaRpcService;
import com.lightgraph.graph.modules.rpc.StorageRpcService;
import com.lightgraph.graph.modules.storage.BackendStorageHandler;
import com.lightgraph.graph.modules.storage.BackendStorageModule;
import com.lightgraph.graph.modules.storage.Batch;
import com.lightgraph.graph.modules.storage.Key;
import com.lightgraph.graph.modules.storage.KeyValue;
import com.lightgraph.graph.server.Server;
import com.lightgraph.graph.server.ServerService;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class StorageManager implements StorageRpcService {

    private static Log LOG = LogFactory.getLog(StorageManager.class);
    private ServerService server;
    private Map<Replication, StorageCommandRunner> commandRunners = new ConcurrentHashMap<>();
    private BackendStorageModule backendStorageModule;
    private ConsensusModule consensusModule;
    private CountDownLatch preapared = new CountDownLatch(2);

    public StorageManager(Server server) {
        this.server = server;
    }

    public void bindBackendStorageModule(BackendStorageModule backendStorageModule) {
        this.backendStorageModule = backendStorageModule;
        preapared.countDown();
    }

    public void reportStorage() {
        List<Replication> replications = this.backendStorageModule.getExistReplication();
        MetaRpcService metaRpcService = server.getMasterLeader();
        List<Replication> metas = replications.stream()
                .filter(r -> r.getGraphName().equals(GraphConstant.META_TABLE_NAME)).collect(Collectors.toList());
        metaRpcService.addReplications(metas);
        replications.removeAll(metas);
        if (replications.size() > 0) {
            metaRpcService.addReplications(replications);
        }
    }

    public void bindConsensusModule(ConsensusModule consensusModule) {
        this.consensusModule = consensusModule;
        preapared.countDown();
    }

    @Override
    public boolean put(String graph, KeyValue keyValue) {
        Replication replication = server.getPartitionLeader(graph, keyValue.getKey());
        byte[] command = keyValue.getBytes();
        byte[] attactedBytes = new byte[command.length + 1];
        System.arraycopy(command, 0, attactedBytes, 1, command.length);
        attactedBytes[0] = StorageOperation.PUT.getOP();
        WriteFuture future = commandRunners.get(replication).writeCommand(attactedBytes);
        if (future == null || !future.get()) {
            LOG.error("write failed!");
        }
        return true;
    }

    @Override
    public boolean batchPut(String graph, Replication replication, Batch batch) {
        byte[] command = batch.getBytes();
        byte[] attactedBytes = new byte[command.length + 1];
        System.arraycopy(command, 0, attactedBytes, 1, command.length);
        attactedBytes[0] = StorageOperation.BATCH_PUT.getOP();
        WriteFuture future = commandRunners.get(replication).writeCommand(attactedBytes);
        if (future == null || !future.get()) {
            LOG.error("write failed!");
        }
        return true;
    }

    @Override
    public Iterator<KeyValue> scan(String graph, Key start) {
        try {
            Set<Replication> replications = server.getReplications(graph, start);
            for (Replication replication : replications) {
                if (replication != null && commandRunners.containsKey(replication)) {
                    return commandRunners.get(replication).scan(start);
                }
            }
            throw new ReplicationNotExistException("replication not exist!");
        } catch (IOException e) {
            LOG.error("get internal error!", e);
            return null;
        }
    }

    @Override
    public KeyValue get(String graph, Key key) {
        try {
            Set<Replication> replications = server.getReplications(graph, key);
            for (Replication replication : replications) {
                if (replication != null && commandRunners.containsKey(replication)) {
                    return commandRunners.get(replication).get(key);
                }
            }
            throw new ReplicationNotExistException("replication not exist!");
        } catch (IOException e) {
            LOG.error("get internal error!", e);
            return null;
        }
    }

    public ConsensusHandler loadConsensusInstance(Replication replication, boolean clean,
            LeaderChangeListener... listeners) {
        ConsensusHandler consensusIO = consensusModule.loadInstance(replication, clean, listeners);
        return consensusIO;
    }

    public BackendStorageHandler getStorageHandler(Replication replication) {
        return backendStorageModule.createStorageIfNotExist(replication);
    }

    public void loadBackendStorage(Replication replication, ConsensusHandler consensusIO) {
        BackendStorageHandler backendStorageHandler = backendStorageModule.createStorageIfNotExist(replication);
        StorageCommandRunner commandRunner = new StorageCommandRunner(consensusIO, null, backendStorageHandler);
        Thread thread = new Thread(commandRunner);
        thread.setDaemon(true);
        thread.setName(replication.getDescription());
        thread.start();
        commandRunners.put(replication, commandRunner);
    }

    public void createStorageIfNotExist(Replication replication) {
        try {
            preapared.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (!commandRunners.containsKey(replication)) {
            MetaRpcService metaRpcService = server.getMasterLeader();
            ConsensusHandler consensusIO = loadConsensusInstance(replication, false,
                    new ReplicationListener(metaRpcService));
            loadBackendStorage(replication, consensusIO);
        }
    }
}