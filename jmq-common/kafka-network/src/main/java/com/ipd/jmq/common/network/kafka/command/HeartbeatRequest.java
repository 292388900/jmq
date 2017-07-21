package com.ipd.jmq.common.network.kafka.command;

/**
 * Created by zhangkepeng on 17-2-10.
 */
public class HeartbeatRequest implements KafkaRequestOrResponse {

    private short version;
    private int correlationId;
    private String clientId;

    private String groupId;
    private int groupGenerationId;
    private String memberId;

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

    public int getGroupGenerationId() {
        return groupGenerationId;
    }

    public void setGroupGenerationId(int groupGenerationId) {
        this.groupGenerationId = groupGenerationId;
    }

    public String getMemberId() {
        return memberId;
    }

    public void setMemberId(String memberId) {
        this.memberId = memberId;
    }

    @Override
    public int type() {
        return KafkaCommandKeys.HEARTBEAT;
    }

    @Override
    public String toString() {
        StringBuilder requestStringBuilder = new StringBuilder();
        requestStringBuilder.append("Name: " + this.getClass().getSimpleName());
        return requestStringBuilder.toString();
    }
}
