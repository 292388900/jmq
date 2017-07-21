package com.ipd.jmq.common.network.kafka.command;

/**
 * Created by zhangkepeng on 16-8-24.
 */
public class UpdateTopicsBrokerRequest implements KafkaRequestOrResponse {

    private short version;
    private int correlationId;
    private String clientId;

    private String topic;
    private int partition;
    private int topicsBrokerType;
    private int lastBrokerId;
    private int controllerEpoch;

    public int getControllerEpoch() {
        return controllerEpoch;
    }

    public void setControllerEpoch(int controllerEpoch) {
        this.controllerEpoch = controllerEpoch;
    }

    public int getLastBrokerId() {
        return lastBrokerId;
    }

    public void setLastBrokerId(int lastBrokerId) {
        this.lastBrokerId = lastBrokerId;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public int getTopicsBrokerType() {
        return topicsBrokerType;
    }

    public void setTopicsBrokerType(int topicsBrokerType) {
        this.topicsBrokerType = topicsBrokerType;
    }

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

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Override
    public int type() {
        return KafkaCommandKeys.UPDATE_TOPICS_BROKER;
    }

    @Override
    public String toString() {
        StringBuilder topicMetadataRequest = new StringBuilder();
        topicMetadataRequest.append("Name: " + this.getClass().getSimpleName());
        topicMetadataRequest.append("; Topic: " + topic);
        return topicMetadataRequest.toString();
    }
}
