package com.ipd.jmq.common.message;

/**
 * 消费者位置
 */
public class ConsumerLocation {
    // 消费者
    private String consumer;
    // 主题
    private String topic;
    // 队列
    private short queueId;
    // 队列位置
    private long queueOffset;
    // 日志位置
    private long journalOffset;

    public String getConsumer() {
        return consumer;
    }

    public void setConsumer(String consumer) {
        this.consumer = consumer;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public short getQueueId() {
        return queueId;
    }

    public void setQueueId(short queueId) {
        this.queueId = queueId;
    }

    public long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public long getJournalOffset() {
        return journalOffset;
    }

    public void setJournalOffset(long journalOffset) {
        this.journalOffset = journalOffset;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ConsumerLocation{");
        sb.append("consumer='").append(consumer).append('\'');
        sb.append(", topic='").append(topic).append('\'');
        sb.append(", queueId=").append(queueId);
        sb.append(", queueOffset=").append(queueOffset);
        sb.append(", journalOffset=").append(journalOffset);
        sb.append('}');
        return sb.toString();
    }
}

