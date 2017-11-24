package com.ipd.jmq.common.network.kafka.command;

/**
 * Created by zhangkepeng on 17-2-16.
 */
public class OffsetQueryRequest implements KafkaRequestOrResponse {

    private short version;
    private int correlationId;
    private String clientId;

    private String topic;
    private int partition;
    private long timestamp;
    private int maxNumOffsets;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getMaxNumOffsets() {
        return maxNumOffsets;
    }

    public void setMaxNumOffsets(int maxNumOffsets) {
        this.maxNumOffsets = maxNumOffsets;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
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

    @Override
    public int type() {
        return KafkaCommandKeys.OFFSET_QUERY;
    }

    @Override
    public String toString() {
        StringBuilder requestStringBuilder = new StringBuilder();
        requestStringBuilder.append("Name: " + this.getClass().getSimpleName());
        return requestStringBuilder.toString();
    }
}
