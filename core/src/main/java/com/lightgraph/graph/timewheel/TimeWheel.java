package com.lightgraph.graph.timewheel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class TimeWheel {
    Log LOG = LogFactory.getLog(TimeWheel.class);

    private int numSlots = 10;
    private long interval = 100;
    private ScheduledExecutorService timeWheelThread;
    private ExecutorService executor;
    private TaskNode[] slots;
    private volatile int cursor = 0;

    public TimeWheel() {
        init(interval, numSlots);
    }

    public TimeWheel(int interval) {
        init(interval, numSlots);
    }

    public TimeWheel(long interval, int numSlots) {
        init(interval, numSlots);
    }

    public void init(long interval, int numSlots) {
        this.numSlots = numSlots;
        this.interval = interval;
        slots = new TaskNode[numSlots];
        for (int i = 0; i < slots.length; i++) {
            TaskNode root = new TaskNode(null);
            root.setSlotKey(i);
            root.pre = root;
            root.next = root;
            slots[i] = root;
        }
        executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        timeWheelThread = Executors.newScheduledThreadPool(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("TimeWheel-Thread");
                return t;
            }
        });
        timeWheelThread.scheduleAtFixedRate(this::advanceTime, 0, this.interval / numSlots, TimeUnit.MILLISECONDS);
    }

    public void advanceTime() {
        cursor = (cursor + 1) % numSlots;
        TaskNode root = slots[cursor];
        if (root.next == root)
            return;
        TaskNode tmp = root;
        synchronized (root) {
            while (true) {
                tmp = tmp.next;
                if (tmp == root)
                    break;
                TaskNode finalTmp = tmp;
                if (finalTmp.isRunning())
                    continue;
                else
                    finalTmp.setRunning(true);
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        finalTmp.run();
                        finalTmp.setRunning(false);
                    }
                });
            }
        }
    }

    public TaskNode addTask(Task task, boolean immediateExecute) {
        if (immediateExecute) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    task.doTask();
                }
            });
            return null;
        }
        TaskNode taskNode = new TaskNode(task);
        addTask(taskNode);
        return taskNode;
    }

    public TaskNode addTask(TaskNode taskNode) {
        int cursor = this.cursor;
        taskNode.setSlotKey(cursor);
        if (taskNode.getTask() == null)
            throw new RuntimeException("invalid task!");
        TaskNode root = slots[cursor];
        while (true) {
            synchronized (root) {
                TaskNode tail = root.pre;
                TaskNode curTail = root.pre;
                if (tail == curTail) {
                    tail.next = taskNode;
                    taskNode.pre = tail;
                    taskNode.next = root;
                    root.pre = taskNode;
                    break;
                }
            }
        }
        LOG.info(String.format("add task successful in slot:%s\t desc:%s", cursor, taskNode.getTask().description()));
        return taskNode;
    }

    public void removeTask(TaskNode task) {
        int oldSlotKey = task.getSlotKey();
        if (task.getTask() == null)
            throw new RuntimeException("should not arrive here!");
        else {
            TaskNode root = findRoot(task);
            synchronized (root) {
                task.pre.next = task.next;
                task.next.pre = task.pre;
                LOG.info("remove node for slot " + task.getSlotKey());
            }
        }
        LOG.info(String.format("remove task successful in slot:%s\t desc:%s", oldSlotKey, task.getTask().description()));
    }

    public TaskNode findRoot(TaskNode task) {
        int slotKey = task.getSlotKey();
        return slots[slotKey];
    }
}
