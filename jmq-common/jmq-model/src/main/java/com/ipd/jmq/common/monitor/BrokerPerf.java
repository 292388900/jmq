package com.ipd.jmq.common.monitor;

import com.ipd.jmq.toolkit.stat.TPStatSlice;
import com.ipd.jmq.toolkit.time.MilliPeriod;
import com.ipd.jmq.toolkit.time.Period;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * broker性能
 *
 * @author xuzhenhua
 */
public class BrokerPerf implements TPStatSlice, Serializable {
    private static final long serialVersionUID = 3704812359800945165L;
    // broker名称
    private String name;
    // broker分组
    private String group;
    // 开始时间
    private long startTime;
    // 结束时间
    private long endTime;
    // 在时间区间内的入队消息条数
    private AtomicLong enQueue = new AtomicLong(0);
    // 在时间区间内的入队消息大小
    private AtomicLong enQueueSize = new AtomicLong(0);
    // 在时间区间内的入队处理时间
    private AtomicLong enQueueTime = new AtomicLong(0);
    // 在时间区间内的出队消息条数
    private AtomicLong deQueue = new AtomicLong(0);
    // 在时间区间内的出队消息大小
    private AtomicLong deQueueSize = new AtomicLong(0);
    // 在时间区间内的出队处理时间
    private AtomicLong deQueueTime = new AtomicLong(0);
    // topic性能列表
    private ConcurrentMap<String, TopicPerf> topicPerfs = new ConcurrentHashMap<String, TopicPerf>();
    // 时间片
    MilliPeriod period = new MilliPeriod();

    public BrokerPerf(String name) {
        this.name = name;
    }

    public BrokerPerf(String name, String group) {
        this.name = name;
        this.group = group;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public AtomicLong getEnQueue() {
        return enQueue;
    }

    public void setEnQueue(AtomicLong enQueue) {
        this.enQueue = enQueue;
    }

    public AtomicLong getEnQueueSize() {
        return enQueueSize;
    }

    public void setEnQueueSize(AtomicLong enQueueSize) {
        this.enQueueSize = enQueueSize;
    }

    public AtomicLong getEnQueueTime() {
        return enQueueTime;
    }

    public void setEnQueueTime(AtomicLong enQueueTime) {
        this.enQueueTime = enQueueTime;
    }

    public AtomicLong getDeQueue() {
        return deQueue;
    }

    public void setDeQueue(AtomicLong deQueue) {
        this.deQueue = deQueue;
    }

    public AtomicLong getDeQueueSize() {
        return deQueueSize;
    }

    public void setDeQueueSize(AtomicLong deQueueSize) {
        this.deQueueSize = deQueueSize;
    }

    public AtomicLong getDeQueueTime() {
        return deQueueTime;
    }

    public void setDeQueueTime(AtomicLong deQueueTime) {
        this.deQueueTime = deQueueTime;
    }

    public ConcurrentMap<String, TopicPerf> getTopicPerfs() {
        return topicPerfs;
    }

    public void setTopicPerfs(ConcurrentMap<String, TopicPerf> topicPerfs) {
        this.topicPerfs = topicPerfs;
    }

    public TopicPerf getAndCreateTopicPerf(String topic) {
        if (topic == null || topic.isEmpty()) {
            return null;
        }
        TopicPerf topicPerf = topicPerfs.get(topic);
        if (topicPerf == null) {
            topicPerf = new TopicPerf(topic);
            TopicPerf old = topicPerfs.putIfAbsent(topic, topicPerf);
            if (old != null) {
                topicPerf = old;
            }
        }
        return topicPerf;
    }

    @Override
    public Period getPeriod() {
        return period;
    }

    @Override
    public void clear() {
        enQueue.set(0);
        enQueueSize.set(0);
        enQueueTime.set(0);
        deQueue.set(0);
        deQueueSize.set(0);
        deQueueTime.set(0);
        for (TopicPerf topicPerf : topicPerfs.values()) {
            topicPerf.clear();
        }
    }
}
