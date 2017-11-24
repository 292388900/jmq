package com.ipd.jmq.common.network.kafka.command;

/**
 * Created by zhangkepeng on 17-2-9.
 */
public class GroupCoordinatorRequest implements KafkaRequestOrResponse{

    private short version;
    private int correlationId;
    private String clientId;

    private String groupId;

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

    @Override
    public String toString() {
        StringBuilder requestStringBuilder = new StringBuilder();
        requestStringBuilder.append("Name: " + this.getClass().getSimpleName());
        requestStringBuilder.append("; groupId: " + groupId);
        return requestStringBuilder.toString();
    }

    @Override
    public int type() {
        return KafkaCommandKeys.GROUP_COORDINATOR;
    }
}
