package com.ipd.jmq.common.network.kafka.command;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Created by zhangkepeng on 17-2-9.
 */
public class JoinGroupResponse implements KafkaRequestOrResponse {

    private int correlationId;

    private short errorCode;
    private int generationId;
    private String groupProtocol;
    private String memberId;
    private String leaderId;
    private Map<String, ByteBuffer> members;

    public int getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(int correlationId) {
        this.correlationId = correlationId;
    }

    public short getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(short errorCode) {
        this.errorCode = errorCode;
    }

    public int getGenerationId() {
        return generationId;
    }

    public void setGenerationId(int generationId) {
        this.generationId = generationId;
    }

    public String getGroupProtocol() {
        return groupProtocol;
    }

    public void setGroupProtocol(String groupProtocol) {
        this.groupProtocol = groupProtocol;
    }

    public String getMemberId() {
        return memberId;
    }

    public void setMemberId(String memberId) {
        this.memberId = memberId;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public Map<String, ByteBuffer> getMembers() {
        return members;
    }

    public void setMembers(Map<String, ByteBuffer> members) {
        this.members = members;
    }

    @Override
    public int type() {
        return KafkaCommandKeys.JOIN_GROUP;
    }

    @Override
    public String toString() {
        StringBuilder responseStringBuilder = new StringBuilder();
        responseStringBuilder.append("Name: " + this.getClass().getSimpleName());
        return responseStringBuilder.toString();
    }
}
