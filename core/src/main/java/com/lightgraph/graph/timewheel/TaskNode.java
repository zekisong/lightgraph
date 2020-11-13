package com.lightgraph.graph.timewheel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

public class TaskNode {

    private static final Log LOG = LogFactory.getLog(TaskNode.class);
    TaskNode next;
    TaskNode pre;
    private int slotKey;
    private Task task;
    private volatile long start;
    private volatile long delay;
    private volatile boolean running = false;
    private List<TimewheelListener> listeners = new ArrayList<>();

    public TaskNode(Task task) {
        this.task = task;
    }

    public Task getTask() {
        return task;
    }

    public int getSlotKey() {
        return slotKey;
    }

    public void setSlotKey(int slotKey) {
        this.slotKey = slotKey;
    }

    public void addListener(TimewheelListener listener) {
        this.listeners.add(listener);
    }

    public long getDelay() {
        return delay;
    }

    public void setDelay(long delay) {
        this.delay = delay;
    }

    public void reset() {
        start = System.currentTimeMillis();
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public void run() {
        long current = System.currentTimeMillis();
        if (start + delay > current)
            return;
        try {
            listeners.forEach(l -> l.onTaskStart());
            task.doTask();
            listeners.forEach(l -> l.onTaskSuccess());
        } catch (Throwable e) {
            LOG.error("task execution failed!", e);
            listeners.forEach(l -> l.onTaskFailed());
        }
        listeners.forEach(l -> l.onTaskFinish());
    }
}
