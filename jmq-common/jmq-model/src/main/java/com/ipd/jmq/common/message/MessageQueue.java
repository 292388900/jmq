package com.ipd.jmq.common.message;

import com.ipd.jmq.toolkit.network.Ipv4;

import java.util.Arrays;

/**
 * 消息队列
 */
public class MessageQueue implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    // 重试队列ID
    public static final short RETRY_QUEUE = 255;
    // 高优先级队列ID
    public static final short HIGH_PRIORITY_QUEUE = 101;
    // 最大普通优先级队列ID
    public static final short MAX_NORMAL_QUEUE = 99;
    // 默认优先级
    public static final byte DEFAULT_PRIORITY = (byte) 4;

    // 地址
    protected byte[] address;
    // Broker名称
    protected String broker;
    // 主题
    protected String topic;
    // 队列ID(>=1)
    protected short queueId;
    //优先级
    protected short priority;

    public MessageQueue(){

    }

    public MessageQueue(String topic, short queueId) {
        if (queueId < 1) {
            throw new IllegalArgumentException("queueId is invalid");
        }
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("topic is invalid");
        }

        this.topic = topic;
        this.queueId = queueId;
    }

    public MessageQueue(String topic, short queueId, short priority) {
        if (queueId < 1) {
            throw new IllegalArgumentException("queueId is invalid");
        }
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("topic is invalid");
        }

        this.topic = topic;
        this.queueId = queueId;
        this.priority = priority;
    }

    /**
     * @param address
     * @param topic
     * @param queueId
     */
    public MessageQueue(byte[] address, String topic, short queueId) {
        this.address = address;
        this.topic = topic;
        this.queueId = queueId;
    }

    /**
     * @param broker
     * @param topic
     * @param queueId
     */
    public MessageQueue(String broker, String topic, short queueId) {
        if (queueId < 1) {
            throw new IllegalArgumentException("queueId is invalid");
        }
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("topic is invalid");
        }
        if (broker == null || broker.isEmpty()) {
            throw new IllegalArgumentException("broker is invalid");
        }
        this.address = Ipv4.toByte(broker);
        this.broker = broker;
        this.topic = topic;
        this.queueId = queueId;
    }

    /**
     * @param broker
     * @param topic
     * @param queueId
     */
    public MessageQueue(String broker, String topic, short queueId, short priority) {
        if (queueId < 0) {
            throw new IllegalArgumentException("queueId is invalid");
        }
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("topic is invalid");
        }
        if (broker == null || broker.isEmpty()) {
            throw new IllegalArgumentException("broker is invalid");
        }
        this.address = Ipv4.toByte(broker);
        this.broker = broker;
        this.topic = topic;
        this.queueId = queueId;
        this.priority = priority;
    }

    public byte[] getAddress() {
        return this.address;
    }

    public String getBroker() {
        if (broker == null && address != null) {
            StringBuilder sb = new StringBuilder();
            Ipv4.toAddress(address, sb);
            broker = sb.toString().replaceAll("[.:]", "_");
        }
        return this.broker;
    }

    public String getTopic() {
        return this.topic;
    }

    public short getQueueId()
    {
        return this.queueId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MessageQueue that = (MessageQueue) o;

        if (queueId != that.queueId) {
            return false;
        }
        if (!Arrays.equals(address, that.address)) {
            return false;
        }
        if (topic != null ? !topic.equals(that.topic) : that.topic != null) {
            return false;
        }

        return true;
    }

    public short getPriority() {
        return priority;
    }

    public void setPriority(short priority) {
        this.priority = priority;
    }

    @Override
    public int hashCode() {
        int result = address != null ? Arrays.hashCode(address) : 0;
        result = 31 * result + (topic != null ? topic.hashCode() : 0);
        result = 31 * result + (int) queueId;
        result = 31 * result + (int) priority;
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        Ipv4.toHex(address, sb);
        sb.append(':').append(topic).append(':').append(queueId).append(':').append(priority);
        return sb.toString();
    }

    public void setAddress(byte[] address) {
        this.address = address;
    }

    public void setBroker(String broker) {
        this.broker = broker;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setQueueId(short queueId) {
        this.queueId = queueId;
    }
}