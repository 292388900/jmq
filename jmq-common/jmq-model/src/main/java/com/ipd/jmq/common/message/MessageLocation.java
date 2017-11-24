package com.ipd.jmq.common.message;


import com.ipd.jmq.toolkit.network.Ipv4;

/**
 * 消息位置
 */
public class MessageLocation extends MessageQueue {
    // 队列偏移
    private long queueOffset;
    // 日志偏移量
    private long journalOffset;

    public MessageLocation() {
    }

    public MessageLocation(byte[] address, String topic, short queueId, long queueOffset) {
        super(address, topic, queueId);
        this.queueOffset = queueOffset;
    }

    public MessageLocation(byte[] address, String topic, short queueId, long queueOffset, long journalOffset) {
        super(address, topic, queueId);
        this.queueOffset = queueOffset;
        this.journalOffset = journalOffset;
    }

    public MessageLocation(String topic, short queueId, long queueOffset) {
        super(topic, queueId);
        this.queueOffset = queueOffset;
    }

    public MessageLocation(String topic, short queueId, long queueOffset, long journalOffset) {
        super(topic, queueId);
        this.queueOffset = queueOffset;
        this.journalOffset = journalOffset;
    }

    public MessageLocation(String broker, String topic, short queueId, long queueOffset) {
        super(broker, topic, queueId);
        this.queueOffset = queueOffset;
    }

    public MessageLocation(BrokerMessage message) {
        super(message.getServerAddress(), message.getTopic(), message.getQueueId());
        this.queueOffset = message.getQueueOffset();
        this.journalOffset = message.getJournalOffset();
    }

    public long getQueueOffset() {
        return this.queueOffset;
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
        StringBuilder sb = new StringBuilder();
        Ipv4.toHex(address, sb);
        sb.append(':').append(topic).append(':').append(queueId).append(':').append(queueOffset);
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        MessageLocation that = (MessageLocation) o;

        if (queueOffset != that.queueOffset) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (int) (queueOffset ^ (queueOffset >>> 32));
        return result;
    }
}