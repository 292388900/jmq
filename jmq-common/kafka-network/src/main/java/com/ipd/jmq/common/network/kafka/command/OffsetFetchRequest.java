package com.ipd.jmq.common.network.kafka.command;

import com.google.common.collect.HashMultimap;

/**
 * Created by zhangkepeng on 16-7-28.
 */
public class OffsetFetchRequest implements KafkaRequestOrResponse {

    private short version;
    private int correlationId;
    private String clientId;

    private String groupId;
    private HashMultimap<String, Integer> topicAndPartitions;

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

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public HashMultimap<String, Integer> getTopicAndPartitions() {
        return topicAndPartitions;
    }

    public void setTopicAndPartitions(HashMultimap<String, Integer> topicAndPartitions) {
        this.topicAndPartitions = topicAndPartitions;
    }

    @Override
    public int type() {
        return KafkaCommandKeys.OFFSET_FETCH;
    }

    @Override
    public String toString() {
        return describe();
    }

    public String describe() {
        StringBuilder offsetFetchRequest = new StringBuilder();
        offsetFetchRequest.append("Name: " + this.getClass().getSimpleName());
        offsetFetchRequest.append("; Version: " + version);
        offsetFetchRequest.append("; CorrelationId: " + correlationId);
        offsetFetchRequest.append("; ClientId: " + clientId);
        offsetFetchRequest.append("; GroupId: " + groupId);
        return offsetFetchRequest.toString();
    }
}
