package com.ipd.jmq.common.network.kafka.command;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Created by zhangkepeng on 17-2-10.
 */
public class SyncGroupRequest implements KafkaRequestOrResponse {

    private short version;
    private int correlationId;
    private String clientId;

    private String groupId;
    private int generationId;
    private String memberId;
    private Map<String, ByteBuffer> groupAssignment;

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

    public int getGenerationId() {
        return generationId;
    }

    public void setGenerationId(int generationId) {
        this.generationId = generationId;
    }

    public String getMemberId() {
        return memberId;
    }

    public void setMemberId(String memberId) {
        this.memberId = memberId;
    }

    public Map<String, ByteBuffer> getGroupAssignment() {
        return groupAssignment;
    }

    public void setGroupAssignment(Map<String, ByteBuffer> groupAssignment) {
        this.groupAssignment = groupAssignment;
    }

    @Override
    public int type() {
        return KafkaCommandKeys.SYNC_GROUP;
    }

    @Override
    public String toString() {
        StringBuilder requestStringBuilder = new StringBuilder();
        requestStringBuilder.append("Name: " + this.getClass().getSimpleName());
        return requestStringBuilder.toString();
    }
}
