package raft.log;

import com.lightgraph.graph.constant.GraphConstant;
import com.lightgraph.graph.exception.GraphException;
import com.lightgraph.graph.modules.consensus.AbstractLeaderChangeListener;
import com.lightgraph.graph.modules.consensus.ConsensusInstance;
import com.lightgraph.graph.modules.consensus.WriteFuture;
import com.lightgraph.graph.timewheel.Task;
import com.lightgraph.graph.timewheel.TaskNode;
import com.lightgraph.graph.timewheel.TimeWheel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import raft.RaftQuorum;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.*;

/**
 * log data structure
 * ____________________________________
 * |     |        |         |         |
 * |term |  index | length  |  data   | ...
 * |_____|________|_________|_________|
 * <p>
 * index data structure
 * ___________________
 * |       |          |
 * | index | position |...
 * |_______|__________|
 */
public class LogSystem extends AbstractLeaderChangeListener {

    private static final Log LOG = LogFactory.getLog(LogSystem.class);
    private String logPath;
    private long segmentSize;
    private TimeWheel timeWheel;
    private TaskNode snapShotTask;
    private AtomicLong commitedEditIndex = new AtomicLong(0);
    private AtomicLong applyedEditIndex = new AtomicLong(0);
    private AtomicLong latestEditIndex = new AtomicLong(0);
    private volatile LogEdit currentEdit;
    private Map<ConsensusInstance, Long> instanceIndexMap = new ConcurrentHashMap<>();
    private ConcurrentSkipListMap<Long, LogSegment> segmentMap = new ConcurrentSkipListMap<>();
    private volatile LogSegment currentSegment;
    private ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private ReentrantLock mutex = new ReentrantLock();
    private Condition commitIndexChange = mutex.newCondition();
    private RaftQuorum quorum;
    private long currentTerm = 0;
    private long preTerm = 0;
    private LinkedBlockingQueue<RaftWriteFuture> futures = new LinkedBlockingQueue();
    private static LogSender sender = new LogSender();

    public LogSystem(RaftQuorum quorum, String logPath, long segmentSize, TimeWheel timeWheel, boolean clean) {
        this.quorum = quorum;
        this.logPath = logPath;
        this.segmentSize = segmentSize;
        this.timeWheel = timeWheel;
        initSegments(clean);
        snapShotTask = this.timeWheel.addTask(new SnapShotTask(), false);
        snapShotTask.setDelay(1000);
        snapShotTask.reset();
    }

    public void initSegments(boolean clean) {
        File logDir = new File(logPath);
        if (!logDir.exists()) {
            logDir.mkdir();
        }
        long largestIndex = -1;
        for (File file : logDir.listFiles()) {
            String name = file.getName();
            String index = "";
            if (name.contains(LogSegment.DATA_SUFFIX) || name.contains(LogSegment.INDEX_SUFFIX)) {
                index = name.split(GraphConstant.SPLIT_FILE_SUFFIX_TOKEN)[0];
            }
            LogSegment segment = new LogSegment(logPath, Long.valueOf(index));
            if (clean) {
                segment.delete();
            } else {
                segment.open();
                segmentMap.put(segment.getStartIndex(), segment);
                if (segment.getEndIndex() > largestIndex && segment.getEndIndex() > segment.getStartIndex()) {
                    if (segment.getStartIndex() != segment.getEndIndex()) {
                        largestIndex = segment.getEndIndex();
                    }
                }
            }
        }
        setLatestEditIndex(largestIndex + 1);
        currentSegment = new LogSegment(logPath, getLatestEditIndex());
        currentSegment.open();
        segmentMap.put(getLatestEditIndex(), currentSegment);
        if (largestIndex >= 0) {
            currentEdit = getLogEdit(largestIndex);
            preTerm = currentEdit.getTerm();
        }
    }

    public void setLatestEditIndex(long index) {
        this.latestEditIndex.set(index);
    }

    public long getLatestEditIndex() {
        return this.latestEditIndex.get();
    }

    public synchronized WriteFuture append(long term, byte[] record) {
        long index = -1;
        RaftWriteFuture future = null;
        Lock lock = rwLock.readLock();
        lock.lock();
        try {
            index = getLatestEditIndex();
            LogEdit edit = new LogEdit(record, preTerm, term, index);
            currentSegment.append(edit);
            currentEdit = edit;
            latestEditIndex.getAndIncrement();
            preTerm = term;
            mutex.lock();
            future = new RaftWriteFuture(edit.getTerm(), edit.getIndex());
            futures.add(future);
            sender.signal();
            return future;
        } catch (Throwable e) {
            LOG.error("master append failed!", e);
            return null;
        } finally {
            mutex.unlock();
            lock.unlock();
        }
    }

    public synchronized boolean append(long preTerm, long term, long index, byte[] record) {
        Lock lock = rwLock.readLock();
        lock.lock();
        try {
            long latestIndex = getLatestEditIndex();
            if (currentEdit == null && latestIndex == 0) {
                LOG.info("first append index:" + index + "\tpre term:" + preTerm + "\tcurrent term:" + term);
                if (index == 0) {
                    LogEdit edit = new LogEdit(record, preTerm, term, index);
                    currentSegment.append(edit);
                    currentEdit = edit;
                    setLatestEditIndex(index + 1);
                    this.preTerm = term;
                    return true;
                } else {
                    return false;
                }
            } else if (index < latestIndex || currentEdit.getTerm() != preTerm) {
                LOG.info("master try to append index:" + index + "\tterm:" + term + "\tpre term:" + preTerm
                        + "\tmy latest edit index:" + latestIndex + "\tterm:" + currentEdit.getTerm());
                long requereIndex = index - 1 >= 0 ? index - 1 : 0;
                currentEdit = getLogEdit(requereIndex);
                truncateTo(requereIndex);
                setLatestEditIndex(requereIndex);
                return false;
            } else if (index > latestIndex) {
                LOG.info("master try to append index:" + index + "\tterm:" + term + "\tpre term:" + preTerm
                        + "\tmy latest edit index:" + latestIndex + "\tterm:" + currentEdit.getTerm() + "\t ignored");
                return false;
            } else {
                LogEdit edit = new LogEdit(record, preTerm, term, index);
                currentSegment.append(edit);
                currentEdit = edit;
                setLatestEditIndex(index + 1);
                this.preTerm = term;
                return true;
            }
        } catch (Throwable e) {
            LOG.error("follower append failed!", e);
            return false;
        } finally {
            lock.unlock();
        }
    }

    public void truncateTo(long index) {
        LogSegment segment = segmentMap.floorEntry(index).getValue();
        if (segment.getEndIndex() < index) {
            throw new GraphException("should not reach here!");
        }
        try {
            segment.truncateTo(index);
            currentSegment = segment;
            LOG.info(String.format("current segment:[%d,%d]", currentSegment.getStartIndex(),
                    currentSegment.getEndIndex()));
            //clean dirty segment
            Map<Long, LogSegment> dirtySegments = segmentMap.tailMap(index + 1);
            for (Long segmentIndex : dirtySegments.keySet()) {
                LogSegment dirtySegment = dirtySegments.get(segmentIndex);
                LOG.info(String.format("remove segment:[%d,%d]", dirtySegment.getStartIndex(),
                        dirtySegment.getEndIndex()));
                dirtySegment.close();
                dirtySegment.delete();
                segmentMap.remove(segmentIndex);
            }
        } catch (Throwable e) {
            LOG.info(String.format("current segment:[%d,%d] failed!", currentSegment.getStartIndex(),
                    currentSegment.getEndIndex()));
        }
    }

    public LogEdit getLogEdit(long index) {
        try {
            LogSegment segment = segmentMap.floorEntry(index).getValue();
            if (index > segment.getEndIndex() || index < segment.getStartIndex()) {
                return null;
            }
            return segment.seekTo(index);
        } catch (Throwable e) {
            LOG.error("get log edit from failed,index:" + index, e);
            throw new GraphException("get log edit failed,segment");
        }
    }

    public void removeNodeIndex(ConsensusInstance instance) {
        if (!instanceIndexMap.containsKey(instance)) {
            Lock lock = rwLock.writeLock();
            lock.lock();
            try {
                if (!instanceIndexMap.containsKey(instance)) {
                    instanceIndexMap.remove(instance);
                }
            } finally {
                lock.unlock();
            }
        }
    }

    public void setNodeIndex(ConsensusInstance instance, long index) {
        if (!instanceIndexMap.containsKey(instance)) {
            Lock lock = rwLock.writeLock();
            lock.lock();
            try {
                if (!instanceIndexMap.containsKey(instance)) {
                    instanceIndexMap.put(instance, index);
                    sender.addInstance(this, instance);
                }
            } finally {
                lock.unlock();
            }
        }
    }

    public void setCommitedEditIndex(long commitIndex) {
        long currentCommitedIndex = getCommitedEditIndex();
        if (commitIndex == currentCommitedIndex && currentCommitedIndex != 0) {
            return;
        } else if (commitIndex < currentCommitedIndex) {
            LOG.error(String.format("try to update commit index:%d,current commited index:%d, ignore...", commitIndex,
                    currentCommitedIndex));
            return;
        }
        try {
            mutex.lock();
            this.commitedEditIndex.set(commitIndex);
            while (true) {
                RaftWriteFuture future = futures.peek();
                if (future != null && future.getTerm() == currentTerm && future.getIndex() <= commitIndex) {
                    future.setSuccess(true);
                    futures.poll();
                } else if (future != null && future.getTerm() != currentTerm && future.getIndex() <= commitIndex) {
                    future.setSuccess(false);
                    futures.poll();
                } else {
                    break;
                }
            }
            commitIndexChange.signalAll();
        } finally {
            mutex.unlock();
        }
    }

    public long getCommitedEditIndex() {
        return commitedEditIndex.get();
    }

    public void clean() {

    }

    public byte[] getUnapplyed() {
        while (true) {
            long applyed = applyedEditIndex.get();
            long commited = getCommitedEditIndex();
            if (applyed <= commited && applyed < getLatestEditIndex()) {
                LogEdit edit = getLogEdit(applyed);
                byte[] r = edit.getData();
                applyedEditIndex.incrementAndGet();
                return r;
            } else {
                try {
                    mutex.lock();
                    commitIndexChange.await(1000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    mutex.unlock();
                }
            }
        }
    }

    @Override
    public synchronized void onLeaderChangeEvent(long term, ConsensusInstance leader) {
        if (this.instance.equals(leader)) {
            Lock lock = rwLock.writeLock();
            lock.lock();
            try {
                this.currentTerm = term;
                setCommitedEditIndex(getLatestEditIndex() - 1);
                instanceIndexMap.clear();
                instanceIndexMap.put(this.instance, getLatestEditIndex());
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void bindInstance(ConsensusInstance instance) {
        this.instance = instance;
        setNodeIndex(this.instance, getLatestEditIndex());
    }

    public long getPreTerm() {
        return this.preTerm;
    }

    public void doSnapshot() {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            currentSegment = new LogSegment(logPath, getLatestEditIndex());
            currentSegment.open();
            segmentMap.put(getLatestEditIndex(), currentSegment);
        } finally {
            lock.unlock();
        }
    }

    class SnapShotTask implements Task {

        @Override
        public String description() {
            return "for log snapshot";
        }

        @Override
        public Object doTask() {
            if (snapShotTask != null) {
                snapShotTask.reset();
            }
            try {
                long size = currentSegment.getDataSize();
                if (size > segmentSize) {
                    doSnapshot();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }


    private Long getInstanceIndex(ConsensusInstance instance) {
        return instanceIndexMap.get(instance);
    }

    private ConsensusInstance getLocalInstance() {
        return instance;
    }

    private void updateInstanceIndex(ConsensusInstance instance, Long index) {
        instanceIndexMap.put(instance, index);
    }

    public boolean isStable() {
        int groupSize = instance.getGroupSize();
        if (!instance.isReady() || instanceIndexMap.size() < groupSize / 2 + 1) {
            LOG.warn(instance.getDescription()
                    + " not stable, isReady:" + instance.isReady()
                    + ", expected group size:" + (groupSize / 2 + 1)
                    + ", actually group size:" + instanceIndexMap.size());
            return false;
        }
        return true;
    }

    private void updateCommitedIndex() {
        int groupSize = instance.getGroupSize();
        List<Long> indices = new ArrayList();
        long latestIndex = getLatestEditIndex();
        instanceIndexMap.put(instance, latestIndex);
        for (Long index : instanceIndexMap.values()) {
            indices.add(index);
        }
        Collections.sort(indices);
        long lowWaterMark = indices.get((groupSize - 1) / 2) - 1;
        if (indices.size() > 0 && lowWaterMark >= 0) {
            setCommitedEditIndex(lowWaterMark);
        }
    }

    private RaftQuorum getQuorum() {
        return quorum;
    }

    static class LogSender {

        private ExecutorService executors;
        private List<Set<ConsensusInstance>> slots;
        private Map<ConsensusInstance, LogSystem> logMap = new ConcurrentHashMap<>();
        private ReentrantLock lock = new ReentrantLock();
        private Condition active = lock.newCondition();

        public LogSender() {
            int cores = Runtime.getRuntime().availableProcessors();
            executors = Executors.newFixedThreadPool(cores);
            slots = new ArrayList<>();
            for (int slot = 0; slot < cores; slot++) {
                slots.add(new HashSet<>());
                int finalSlot = slot;
                executors.submit(() -> {
                    Thread.currentThread().setName(String.format("LogSender[slot:%d]", finalSlot));
                    Set<ConsensusInstance> consensusInstances = slots.get(finalSlot);
                    while (true) {
                        for (ConsensusInstance instance : consensusInstances) {
                            LogSystem log = logMap.get(instance);
                            Long endIndex = log.getLatestEditIndex();
                            Long startIndex = log.getInstanceIndex(instance);

                            if (!log.getQuorum().isLeader()
                                    || !log.isStable()
                                    || startIndex == null) {
                                continue;
                            }
                            if (instance.equals(log.getLocalInstance())) {
                                log.updateCommitedIndex();
                                continue;
                            }
                            if (startIndex > endIndex) {
                                log.updateInstanceIndex(instance, endIndex - 1);
                                continue;
                            }
                            long nextIndex = startIndex;
                            for (long index = startIndex; index < endIndex; index++) {
                                LogEdit edit = log.getLogEdit(index);
                                boolean isok = false;
                                try {
                                    isok = log.getQuorum()
                                            .appendLogSync(instance, edit.getPreTerm(), edit.getTerm(), edit.getIndex(),
                                                    edit.getData());
                                } catch (Throwable e) {
                                    LOG.error("sync index:" + index + "to:" + instance + " falied!", e);
                                }
                                if (!isok) {
                                    LOG.info("sync index:" + index + " to " + instance + " falied!\t version:" + log
                                            .getInstanceIndex(instance));
                                    nextIndex = index - 1 >= 0 ? index - 1 : 0;
                                    break;
                                }
                                nextIndex = index + 1;
                            }
                            log.updateInstanceIndex(instance, nextIndex);
                            log.updateCommitedIndex();
                        }
                        lock.lock();
                        try {
                            active.await(1000, TimeUnit.MILLISECONDS);
                        } finally {
                            lock.unlock();
                        }
                    }
                });
            }
        }

        public synchronized void signal() {
            lock.lock();
            try {
                active.signalAll();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
            }
        }

        public void addInstance(LogSystem logSystem, ConsensusInstance instance) {
            int slot = Math.abs(instance.hashCode()) % slots.size();
            slots.get(slot).add(instance);
            logMap.put(instance, logSystem);
        }
    }
}