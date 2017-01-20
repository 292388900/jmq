package com.ipd.jmq.common.monitor;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * topic性能
 *
 * @author xuzhenhua
 */
public class TopicPerf implements Serializable {
    private static final long serialVersionUID = -7830087948268640281L;
    // 主题
    private String topic;
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
    // app性能列表
    private ConcurrentMap<String, AppPerf> appPerfs = new ConcurrentHashMap<String, AppPerf>();

    public TopicPerf() {
    }

    public TopicPerf(String topic) {
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
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

    public ConcurrentMap<String, AppPerf> getAppPerfs() {
        return appPerfs;
    }

    public void setAppPerfs(ConcurrentMap<String, AppPerf> appPerfs) {
        this.appPerfs = appPerfs;
    }

    public AppPerf getAndCreateAppStat(String app) {
        if (app == null || app.isEmpty()) {
            return null;
        }
        AppPerf appPerf = appPerfs.get(app);
        if (appPerf == null) {
            appPerf = new AppPerf(app);
            AppPerf old = appPerfs.putIfAbsent(app, appPerf);
            if (old != null) {
                appPerf = old;
            }
        }
        return appPerf;
    }

    public void clear() {
        enQueue.set(0);
        enQueueSize.set(0);
        enQueueTime.set(0);
        deQueue.set(0);
        deQueueSize.set(0);
        deQueueTime.set(0);
        for (AppPerf appPerf : appPerfs.values()) {
            appPerf.clear();
        }
    }
}
