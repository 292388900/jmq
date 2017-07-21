package com.ipd.jmq.common.network.kafka.command;

import com.ipd.jmq.common.network.kafka.model.PartitionOffsetsResponse;

import java.util.Map;

/**
 * Created by zhangkepeng on 16-8-4.
 */
public class OffsetResponse implements KafkaRequestOrResponse {

    private int correlationId;
    private Map<String, Map<Integer, PartitionOffsetsResponse>> offsetsResponseMap;

    public int getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(int correlationId) {
        this.correlationId = correlationId;
    }

    public Map<String, Map<Integer, PartitionOffsetsResponse>> getOffsetsResponseMap() {
        return offsetsResponseMap;
    }

    public void setOffsetsResponseMap(Map<String, Map<Integer, PartitionOffsetsResponse>> offsetsResponseMap) {
        this.offsetsResponseMap = offsetsResponseMap;
    }

    @Override
    public int type() {
        return KafkaCommandKeys.LIST_OFFSETS;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Name: " + this.getClass().getSimpleName());
        return builder.toString();
    }
}
