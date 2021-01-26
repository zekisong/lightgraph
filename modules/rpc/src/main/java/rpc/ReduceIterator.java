package rpc;

import com.lightgraph.graph.exception.GraphException;
import com.lightgraph.graph.modules.storage.KeyValue;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ReduceIterator implements Iterator<KeyValue> {

    private ReentrantLock lock = new ReentrantLock();
    private Condition fetched = lock.newCondition();
    private Condition empty = lock.newCondition();
    private Iterator<KeyValue> kvs;
    private volatile boolean closed = false;

    @Override
    public boolean hasNext() {
        while (true) {
            if (kvs == null) {
                if (closed) {
                    return false;
                }
                lock.lock();
                try {
                    fetched.await();
                } catch (InterruptedException e) {
                    throw new GraphException("get next failed!", e);
                } finally {
                    lock.unlock();
                }
            } else if (!kvs.hasNext()) {
                if (closed) {
                    return false;
                }
                lock.lock();
                try {
                    empty.signalAll();
                    fetched.await();
                } catch (InterruptedException e) {
                    throw new GraphException("get next failed!", e);
                } finally {
                    lock.unlock();
                }
                if (closed && !kvs.hasNext()) {
                    return false;
                } else {
                    return true;
                }
            } else {
                return true;
            }
        }
    }

    public void add(List<KeyValue> kvs) {
        lock.lock();
        try {
            this.kvs = kvs.iterator();
            fetched.signalAll();
            empty.await();
        } catch (InterruptedException e) {
            throw new GraphException("wait consumer finished failed!", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public KeyValue next() {
        return kvs.next();
    }

    public void close() {
        lock.lock();
        try {
            closed = true;
            fetched.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
