package com.ipd.jmq.common.network.kafka.command;

import com.ipd.jmq.common.network.kafka.model.PartitionOffsetsResponse;

/**
 * Created by zhangkepeng on 17-2-16.
 */
public class OffsetQueryResponse implements KafkaRequestOrResponse {

    private int correlationId;

    private PartitionOffsetsResponse partitionOffsetsResponse;

    public PartitionOffsetsResponse getPartitionOffsetsResponse() {
        return partitionOffsetsResponse;
    }

    public void setPartitionOffsetsResponse(PartitionOffsetsResponse partitionOffsetsResponse) {
        this.partitionOffsetsResponse = partitionOffsetsResponse;
    }

    public int getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(int correlationId) {
        this.correlationId = correlationId;
    }

    @Override
    public int type() {
        return KafkaCommandKeys.OFFSET_QUERY;
    }

    @Override
    public String toString() {
        StringBuilder responseStringBuilder = new StringBuilder();
        responseStringBuilder.append("Name: " + this.getClass().getSimpleName());
        return responseStringBuilder.toString();
    }
}
