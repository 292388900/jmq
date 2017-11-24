package com.ipd.jmq.common.network.kafka.command;

import java.util.Set;

/**
 * Created by zhangkepeng on 16-7-27.
 */
public class TopicMetadataRequest implements KafkaRequestOrResponse {

    private short version;
    private int correlationId;
    private String clientId;

    private Set<String> topics;

    public short getVersion() {
        return version;
    }

    public void setVersion(short version) {
        this.version = version;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public int getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(int correlationId) {
        this.correlationId = correlationId;
    }

    public void setTopics(Set<String> topics) {
        this.topics = topics;
    }

    public Set<String> getTopics() {
        return topics;
    }

    @Override
    public int type() {
        return KafkaCommandKeys.METADATA;
    }

    @Override
    public String toString() {
        StringBuilder topicMetadataRequest = new StringBuilder();
        topicMetadataRequest.append("Name: " + this.getClass().getSimpleName());
        if (topics != null && !topics.isEmpty()) {
            for (String topic : topics) {
                topicMetadataRequest.append("; Topic: " + topic);
            }
        }
        return topicMetadataRequest.toString();
    }
}
