package com.ipd.jmq.common.cluster;


import com.ipd.jmq.toolkit.time.SystemClock;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 顺序Broker状态
 * <p/>
 * 顺序消息列表sequentialBrokers 用一个队列存放当前可写和可读的Broker,
 * 队首的Broker是可写的，其他的Broker都是可读的，
 * 但为了保证顺序只能是队尾的Broker上的消息消费完了能向前按顺序读取其他的Broker上的消息.
 *
 * @author lindeqiang
 * @since 2015/8/19 13:50
 */
public class SequentialBrokerState {
    // 时间戳
    private long timestamp = SystemClock.now();
    // 顺序消息Broker列表
    private Deque<SequentialBroker> sequentialBrokers = new LinkedList<SequentialBroker>();
    // 可重入读写锁
    final ReadWriteLock lock = new ReentrantReadWriteLock();

    public SequentialBrokerState() {
        //nothing to do
    }


    /**
     * 获取可读的Broker
     *
     * @return 可读的Broker
     */
    public SequentialBroker fetchReadableBroker() {
        lock.readLock().lock();
        try {
            return sequentialBrokers.peekLast();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 获取可写的Broker
     *
     * @return 可写的Broker
     */
    public SequentialBroker fetchWritableBroker() {
        lock.readLock().lock();
        try {
            return sequentialBrokers.peek();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 添加可写的Broker
     *
     * @param writableGroup 可写的Broker
     */
    public void addWritableBroker(SequentialBroker writableGroup) {
        lock.writeLock().lock();
        try {
            // 确保当前列表里没有再添加
            if (!sequentialBrokers.contains(writableGroup)) {
                sequentialBrokers.addFirst(writableGroup);
                updateTimeStamp(timestamp);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 将无积压的Broker从写列表中移除，用于以后备选.正在发送的Broker默认有积压，不能移除
     *
     * @param idleBroker 无积压的Broker
     * @return
     */
    public boolean removeIdledGroup(SequentialBroker idleBroker) {
        lock.writeLock().lock();
        try {
            if (sequentialBrokers.size() > 1 && !idleBroker.equals(sequentialBrokers.getFirst())) {
                updateTimeStamp(-1);
                return sequentialBrokers.removeLastOccurrence(idleBroker);
            }
        } finally {
            lock.writeLock().unlock();
        }
        return false;
    }

    /**
     * 将Broker从写列表中移除，用于以后备选.或者说消息已经不再分配到该broker上
     *
     * @param broker Broker
     * @return
     */
    public boolean removeBoker(SequentialBroker broker) {
        lock.writeLock().lock();
        try {
            if (sequentialBrokers.size() > 0) {
                updateTimeStamp(-1);
                return sequentialBrokers.remove(broker);
            }
        } finally {
            lock.writeLock().unlock();
        }
        return false;
    }

 /**
     * 将Broker从写列表中移除，用于以后备选.或者说消息已经不再分配到该broker上
     *
     * @param  brokers 要移除的Broker列表
     * @return
     */
    public boolean removeBroker(List<SequentialBroker> brokers) {
        if (brokers == null || brokers.isEmpty()){
            return false;
        }
        lock.writeLock().lock();
        try {
            if (sequentialBrokers.size() > 0) {
                updateTimeStamp(-1);
                return sequentialBrokers.removeAll(brokers);
            }
        } finally {
            lock.writeLock().unlock();
        }
        return false;
    }


    /**
     * 将Broker从写列表中移除，用于以后备选.或者说消息已经不再分配到该broker上
     *
     */
    public boolean removeAllBroker() {
        lock.writeLock().lock();
        try {
            if (sequentialBrokers.size() > 0) {
                updateTimeStamp(-1);
                sequentialBrokers.clear();
                return true;
            }
        } finally {
            lock.writeLock().unlock();
        }

        return false;
    }

    /**
     * 拷贝下当前的状态
     *
     * @return 当前顺序broker的状态
     */
    public SequentialBrokerState clone() {
        lock.writeLock().lock();
        try {
            SequentialBrokerState target = new SequentialBrokerState();
            target.setTimestamp(this.timestamp);
            target.setSequentialBrokers(new LinkedList<SequentialBroker>(sequentialBrokers));
            return target;
        } finally {
            lock.writeLock().unlock();
        }

    }

    /**
     * 是否具有可写的Broker
     *
     * @return Y/N
     */
    public boolean hasWritableBroker() {
        lock.readLock().lock();
        try {
            return sequentialBrokers != null && sequentialBrokers.size() > 0;
        } finally {
         lock.readLock().unlock();
        }
    }

    /**
     * 是否可以作为候选者
     *
     * @param group Broker分组
     * @return Y/N
     */
    public boolean isWriteCandidate(String group) {
        lock.readLock().lock();
        try {
            return !sequentialBrokers.contains(new SequentialBroker(group));
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 是否积压
     *
     * @param group
     * @return
     */
    public boolean hasBacklogs(String group){
        lock.readLock().lock();
        try {
            return sequentialBrokers.contains(new SequentialBroker(group));
        } finally {
            lock.readLock().unlock();
        }
    }


    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Deque<SequentialBroker> getSequentialBrokers() {
        return sequentialBrokers;
    }

    public void setSequentialBrokers(Deque<SequentialBroker> sequentialBrokers) {
        if (sequentialBrokers == null) {
            return;
        }
        lock.writeLock().lock();
        try {
            this.sequentialBrokers = sequentialBrokers;
            updateTimeStamp(-1);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void updateTimeStamp(long timestamp) {
        if (timestamp > 0) {
            this.timestamp = timestamp;
        } else {
            this.timestamp = SystemClock.now();
        }
    }


    //顺序Broker
    public static class SequentialBroker {
        private String group;

        public SequentialBroker() {
            //nothing to do
        }

        public SequentialBroker(String group) {
            this.group = group;
        }

        public String getGroup() {
            return group;
        }

        public void setGroup(String group) {
            this.group = group;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SequentialBroker that = (SequentialBroker) o;

            if (group != null ? !group.equals(that.group) : that.group != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return group != null ? group.hashCode() : 0;
        }

        @Override
        public String toString() {
            return "SequentialBroker{" +
                    "group='" + group + '\'' +
                    '}';
        }
    }


    @Override
    public String toString() {
        return "SequentialBrokerState{" +
                "sequentialBrokers=" + sequentialBrokers +
                '}';
    }
}
