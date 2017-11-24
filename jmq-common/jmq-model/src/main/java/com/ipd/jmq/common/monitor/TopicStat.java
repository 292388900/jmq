package com.ipd.jmq.common.monitor;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * topic统计信息
 *
 * @author xuzhenhua
 */
public class TopicStat implements Serializable {
    private static final long serialVersionUID = -2410477972367211215L;
    // 主题
    private String topic;
    // 生产者数量
    private AtomicLong producer = new AtomicLong(0);
    // 消费者数量
    private AtomicLong consumer = new AtomicLong(0);
    // 服务端入队消息数量
    private AtomicLong enQueue = new AtomicLong(0);
    // 服务端入队消息大小(单位字节)
    private AtomicLong enQueueSize = new AtomicLong(0);
    // 服务端出队消息大小(单位字节)
    private AtomicLong deQueueSize = new AtomicLong(0);
    // 服务端出队消息数量
    private AtomicLong deQueue = new AtomicLong(0);
    // 应用统计
    private ConcurrentMap<String, AppStat> appStats = new ConcurrentHashMap<String, AppStat>();

    public TopicStat() {
    }

    public TopicStat(String topic) {
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public AtomicLong getProducer() {
        return producer;
    }

    public void setProducer(AtomicLong producer) {
        this.producer = producer;
    }

    public AtomicLong getConsumer() {
        return consumer;
    }

    public void setConsumer(AtomicLong consumer) {
        this.consumer = consumer;
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

    public AtomicLong getDeQueueSize() {
        return deQueueSize;
    }

    public void setDeQueueSize(AtomicLong deQueueSize) {
        this.deQueueSize = deQueueSize;
    }

    public AtomicLong getDeQueue() {
        return deQueue;
    }

    public void setDeQueue(AtomicLong deQueue) {
        this.deQueue = deQueue;
    }

    public ConcurrentMap<String, AppStat> getAppStats() {
        return appStats;
    }

    public void setAppStats(ConcurrentMap<String, AppStat> appStats) {
        this.appStats = appStats;
    }

    public AppStat getAndCreateAppStat(String app) {
        if (app == null || app.isEmpty()) {
            return null;
        }
        AppStat appStat = appStats.get(app);
        if (appStat == null) {
            appStat = new AppStat(app);
            AppStat old = appStats.putIfAbsent(app, appStat);
            if (old != null) {
                appStat = old;
            }
        }
        return appStat;
    }

    public AppStat getAppStat(String app){
        if (app == null || app.isEmpty()) {
            return null;
        }
        AppStat appStat = appStats.get(app);

        return appStat;
    }
}
