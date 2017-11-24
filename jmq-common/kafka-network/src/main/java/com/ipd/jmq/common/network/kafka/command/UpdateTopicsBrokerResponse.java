package com.ipd.jmq.common.network.kafka.command;

/**
 * Created by zhangkepeng on 16-8-24.
 */
public class UpdateTopicsBrokerResponse implements KafkaRequestOrResponse {

    private String topic;
    private short errorCode;
    private int correlationId;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public short getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(short errorCode) {
        this.errorCode = errorCode;
    }

    public int getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(int correlationId) {
        this.correlationId = correlationId;
    }

    @Override
    public int type() {
        return KafkaCommandKeys.UPDATE_TOPICS_BROKER;
    }
}
