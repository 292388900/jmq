package com.ipd.jmq.server.broker.offset;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 偏移量
 */
public class Offset implements Cloneable {
    // 主题
    private transient String topic;
    // 消费者
    private transient String consumer;
    // 队列
    private transient int queueId;
    // 订阅开始位置
    private AtomicLong subscribeOffset = new AtomicLong(0);
    // 拉取位置
    private AtomicLong pullOffset = new AtomicLong(0);
    // 应答位置
    private AtomicLong ackOffset = new AtomicLong(0);

    public Offset() {
    }

    public Offset(String topic, int queueId, String consumer) {
        this.topic = topic;
        this.consumer = consumer;
        this.queueId = queueId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getConsumer() {
        return consumer;
    }

    public void setConsumer(String consumer) {
        this.consumer = consumer;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(short queueId) {
        this.queueId = queueId;
    }

    public AtomicLong getSubscribeOffset() {
        return subscribeOffset;
    }

    public void setSubscribeOffset(AtomicLong subscribeOffset) {
        this.subscribeOffset = subscribeOffset;
    }

    public AtomicLong getPullOffset() {
        return pullOffset;
    }

    public void setPullOffset(AtomicLong pullOffset) {
        this.pullOffset = pullOffset;
    }

    public AtomicLong getAckOffset() {
        return ackOffset;
    }

    public void setAckOffset(AtomicLong ackOffset) {
        this.ackOffset = ackOffset;
    }

    /**
     * 更新偏移量
     *
     * @param offset 偏移量
     */
    public void updateOffset(Offset offset) {
        if (offset == null) {
            return;
        }
        subscribeOffset.compareAndSet(0, offset.getSubscribeOffset().get());
        compareGreaterAndSet(ackOffset, offset.getAckOffset().get());
        compareGreaterAndSet(pullOffset, offset.getPullOffset().get());
    }

    /**
     * 重置应答位置
     *
     * @param ackOffset 应答位置
     */
    public void resetAckOffset(long ackOffset) {
        this.ackOffset.set(ackOffset);
    }

    /**
     * 比较偏移量
     *
     * @param atomic 原子
     * @param offset 偏移量
     */
    public void compareGreaterAndSet(AtomicLong atomic, long offset) {
        boolean flag;
        do {
            long old = atomic.get();
            if (old < offset) {
                flag = atomic.compareAndSet(old, offset);
            } else {
                flag = true;
            }
        } while (!flag);
    }

    @Override
    public Offset clone() throws CloneNotSupportedException {
        return (Offset) super.clone();
    }

}