package com.ipd.jmq.common.network.kafka.command;

import com.ipd.jmq.common.network.kafka.model.OffsetAndMetadata;

import java.util.Map;

/**
 * Created by zhangkepeng on 16-7-28.
 */
public class OffsetCommitRequest implements KafkaRequestOrResponse{

    public static final int DEFAULT_GENERATION_ID = -1;
    public static final String DEFAULT_CONSUMER_ID = "";
    public static final long DEFAULT_TIMESTAMP = -1L;

    private short version;
    private int correlationId;
    private String clientId;

    private String groupId;
    private Map<String, Map<Integer, OffsetAndMetadata>> offsetAndMetadataMap;

    private int groupGenerationId;
    private String memberId;
    private long retentionTime;

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

    public String getMemberId() {
        return memberId;
    }

    public void setMemberId(String memberId) {
        this.memberId = memberId;
    }

    public int getGroupGenerationId() {
        return groupGenerationId;
    }

    public void setGroupGenerationId(int groupGenerationId) {
        this.groupGenerationId = groupGenerationId;
    }

    public Map<String, Map<Integer, OffsetAndMetadata>> getOffsetAndMetadataMap() {
        return offsetAndMetadataMap;
    }

    public void setOffsetAndMetadataMap(Map<String, Map<Integer, OffsetAndMetadata>> offsetAndMetadataMap) {
        this.offsetAndMetadataMap = offsetAndMetadataMap;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public long getRetentionTime() {
        return retentionTime;
    }

    public void setRetentionTime(long retentionTime) {
        this.retentionTime = retentionTime;
    }

    @Override
    public String toString() {
        StringBuilder offsetCommitRequest = new StringBuilder();
        offsetCommitRequest.append("Name: " + this.getClass().getSimpleName());
        offsetCommitRequest.append("; GroupId: " + groupId);
        offsetCommitRequest.append("; GroupGenerationId: " + groupGenerationId);
        offsetCommitRequest.append("; memberId: " + memberId);
        return offsetCommitRequest.toString();
    }

    @Override
    public int type() {
        return KafkaCommandKeys.OFFSET_COMMIT;
    }
}
