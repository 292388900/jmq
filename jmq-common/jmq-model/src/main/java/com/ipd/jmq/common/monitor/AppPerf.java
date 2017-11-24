package com.ipd.jmq.common.monitor;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * app性能
 *
 * @author xuzhenhua
 */
public class AppPerf implements Serializable {
    private static final long serialVersionUID = 1870681262626339195L;
    // 应用代码
    private String app;

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

    public AppPerf() {
    }

    public AppPerf(String app) {
        this.app = app;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
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

    public void clear() {
        enQueue.set(0);
        enQueueSize.set(0);
        enQueueTime.set(0);
        deQueue.set(0);
        deQueueSize.set(0);
        deQueueTime.set(0);
    }
}
