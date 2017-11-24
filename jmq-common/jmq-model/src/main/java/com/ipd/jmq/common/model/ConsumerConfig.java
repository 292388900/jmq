package com.ipd.jmq.common.model;


import com.ipd.jmq.toolkit.retry.RetryPolicy;
import java.io.Serializable;

/**
 * 消费者配置
 *
 * @author lindeqiang
 */
public class ConsumerConfig implements Serializable {
    // 预取数量（针对拉模式有效）
    private short prefetchSize = 100;
    // 长轮询
    private int longPull = 5000; // dynamic
    // 拉取时间间隔(ms)
    private int pullInterval = 0;
    // 空闲休息时间(ms)
    private int pullEmptySleep = 1000; // dynamic
    // 从服务器拉取超时(ms)
    private int pullTimeout = 10000; // dynamic
    //最大并发数
    private int maxConcurrent = 0; // dynamic
    //最小并发数
    private int minConcurrent = 0; // dynamic
    // 重试策略  dynamic
    private RetryPolicy retryPolicy = new RetryPolicy(1000, 1000, 2, false, 2.0, 0);
    //自动开始消费
    private boolean autoStart = true;
    // 广播offset持久化间隔
    private int persistConsumerOffsetInterval = 1000 * 5;
    // 广播消费者实例名称
    private String clientName;

    public short getPrefetchSize() {
        return prefetchSize;
    }

    public void setPrefetchSize(short prefetchSize) {
        if (prefetchSize > 0) {
            this.prefetchSize = prefetchSize;
        }
    }

    public String getClientName() {
        return clientName;
    }

    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    public int getLongPull() {
        return this.longPull;
    }

    public void setLongPull(int longPull) {
        this.longPull = longPull;
    }

    public int getPullInterval() {
        return this.pullInterval;
    }

    public void setPullInterval(int pullInterval) {
        if (pullInterval > 0) {
            this.pullInterval = pullInterval;
        }
    }

    public int getPullEmptySleep() {
        return this.pullEmptySleep;
    }

    public void setPullEmptySleep(int pullEmptySleep) {
        if (pullEmptySleep > 0) {
            this.pullEmptySleep = pullEmptySleep;
        }
    }

    public int getPullTimeout() {
        return this.pullTimeout;
    }

    public void setPullTimeout(int pullTimeout) {
        if (pullTimeout > 0) {
            this.pullTimeout = pullTimeout;
        }
    }

    public RetryPolicy getRetryPolicy() {
        return this.retryPolicy;
    }

    public void setRetryPolicy(RetryPolicy retryPolicy) {
        if (retryPolicy != null) {
            this.retryPolicy = retryPolicy;
        }
    }

    public void setPersistConsumerOffsetInterval(int persistConsumerOffsetInterval) {
        this.persistConsumerOffsetInterval = persistConsumerOffsetInterval;
    }

    public int getPersistConsumerOffsetInterval() {
        return persistConsumerOffsetInterval;
    }

    public boolean isAutoStart() {
        return autoStart;
    }

    public void setAutoStart(boolean autoStart) {
        this.autoStart = autoStart;
    }

    public int getMaxConcurrent() {
        return maxConcurrent;
    }

    public void setMaxConcurrent(int maxConcurrent) {
        this.maxConcurrent = maxConcurrent;
    }

    public int getMinConcurrent() {
        return minConcurrent;
    }

    public void setMinConcurrent(int minConcurrent) {
        this.minConcurrent = minConcurrent;
    }

}
