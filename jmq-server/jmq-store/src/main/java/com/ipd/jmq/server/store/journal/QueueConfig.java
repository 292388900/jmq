package com.ipd.jmq.server.store.journal;

import com.ipd.jmq.toolkit.validate.annotation.Range;

/**
 * 队列配置
 * Created by hexiaofeng on 16-7-12.
 */
public class QueueConfig {
    // 队列大小
    @Range(min = 1, max = Integer.MAX_VALUE)
    private int size;
    // 入队超时时间
    @Range(min = 1, max = Long.MAX_VALUE)
    private long timeout = 100;

    public QueueConfig() {
    }

    public QueueConfig(int size, long timeout) {
        this.size = size;
        this.timeout = timeout;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    /**
     * 构造器
     */
    public static class Builder {

        QueueConfig config = new QueueConfig();

        public static Builder build() {
            return new Builder();
        }

        public Builder size(int size) {
            config.setSize(size);
            return this;
        }

        public Builder dataSize(int timeout) {
            config.setTimeout(timeout);
            return this;
        }

        public QueueConfig create() {
            return config;
        }

    }
}
