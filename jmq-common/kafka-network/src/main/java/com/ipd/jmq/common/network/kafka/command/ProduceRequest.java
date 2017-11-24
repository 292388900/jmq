package com.ipd.jmq.common.network.kafka.command;

import com.ipd.jmq.common.network.kafka.message.ByteBufferMessageSet;

import java.util.Map;

/**
 * Created by zhangkepeng on 16-7-27.
 */
public class ProduceRequest implements KafkaRequestOrResponse {

    private short version;
    private int correlationId;
    private String clientId;

    private short requiredAcks;
    private int ackTimeoutMs;

    private Map<String, Map<Integer, ByteBufferMessageSet>> topicPartitionMessages;

    public short getVersion() {
        return version;
    }

    public void setVersion(short version) {
        this.version = version;
    }

    public int getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(int correlationId) {
        this.correlationId = correlationId;
    }

    public String getClientId() {
        return clientId;
    }

    public short getRequiredAcks() {
        return requiredAcks;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public void setRequiredAcks(short requiredAcks) {
        this.requiredAcks = requiredAcks;
    }

    public void setAckTimeoutMs(int ackTimeoutMs) {
        this.ackTimeoutMs = ackTimeoutMs;
    }

    public int getAckTimeoutMs() {
        return ackTimeoutMs;
    }

    public Map<String, Map<Integer, ByteBufferMessageSet>> getTopicPartitionMessages() {
        return topicPartitionMessages;
    }

    public void setTopicPartitionMessages(Map<String, Map<Integer, ByteBufferMessageSet>> topicPartitionMessages) {
        this.topicPartitionMessages = topicPartitionMessages;
    }

    @Override
    public int type() {
       return KafkaCommandKeys.PRODUCE;
    }

    @Override
    public String toString() {
        return describe();
    }

    private String describe() {
        StringBuilder producerRequest = new StringBuilder();
        producerRequest.append("Name: " + this.getClass().getSimpleName());
        producerRequest.append("; RequiredAcks: " + requiredAcks);
        producerRequest.append("; AckTimeoutMs: " + ackTimeoutMs + " ms");
        return producerRequest.toString();
    }
}
