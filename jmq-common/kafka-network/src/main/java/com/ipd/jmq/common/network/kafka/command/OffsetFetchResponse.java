package com.ipd.jmq.common.network.kafka.command;

import com.ipd.jmq.common.network.kafka.model.OffsetMetadataAndError;

import java.util.Map;

/**
 * Created by zhangkepeng on 16-8-4.
 */
public class OffsetFetchResponse implements KafkaRequestOrResponse{

    private int correlationId;
    private Map<String, Map<Integer, OffsetMetadataAndError>> topicMetadataAndErrors;

    public Map<String, Map<Integer, OffsetMetadataAndError>> getTopicMetadataAndErrors() {
        return topicMetadataAndErrors;
    }

    public void setTopicMetadataAndErrors(Map<String, Map<Integer, OffsetMetadataAndError>> topicMetadataAndErrors) {
        this.topicMetadataAndErrors = topicMetadataAndErrors;
    }

    public int getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(int correlationId) {
        this.correlationId = correlationId;
    }

    @Override
    public int type() {
        return KafkaCommandKeys.OFFSET_FETCH;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Name: " + this.getClass().getSimpleName());
        return builder.toString();
    }
}
