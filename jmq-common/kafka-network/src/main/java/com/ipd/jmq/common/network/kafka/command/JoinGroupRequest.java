package com.ipd.jmq.common.network.kafka.command;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by zhangkepeng on 17-2-9.
 */
public class JoinGroupRequest implements KafkaRequestOrResponse {

    public static final String UNKNOWN_MEMBER_ID = "";

    private short version;
    private int correlationId;
    private String clientId;

    private String groupId;
    private int sessionTimeout;
    private String memberId;
    private String protocolType;
    private List<ProtocolMetadata> groupProtocols;

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

    public int getSessionTimeout() {
        return sessionTimeout;
    }

    public void setSessionTimeout(int sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    public String getMemberId() {
        return memberId;
    }

    public void setMemberId(String memberId) {
        this.memberId = memberId;
    }

    public String getProtocolType() {
        return protocolType;
    }

    public void setProtocolType(String protocolType) {
        this.protocolType = protocolType;
    }

    public List<ProtocolMetadata> getGroupProtocols() {
        return groupProtocols;
    }

    public void setGroupProtocols(List<ProtocolMetadata> groupProtocols) {
        this.groupProtocols = groupProtocols;
    }

    public static class ProtocolMetadata {
        private final String name;
        private final ByteBuffer metadata;

        public ProtocolMetadata(String name, ByteBuffer metadata) {
            this.name = name;
            this.metadata = metadata;
        }

        public String name() {
            return name;
        }

        public ByteBuffer metadata() {
            return metadata;
        }
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
        return KafkaCommandKeys.JOIN_GROUP;
    }
}
