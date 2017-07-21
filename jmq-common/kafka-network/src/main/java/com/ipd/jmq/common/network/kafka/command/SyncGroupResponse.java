package com.ipd.jmq.common.network.kafka.command;

import java.nio.ByteBuffer;

/**
 * Created by zhangkepeng on 17-2-10.
 */
public class SyncGroupResponse implements KafkaRequestOrResponse {

    private int correlationId;
    private short errorCode;
    private ByteBuffer memberState;

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

    public ByteBuffer getMemberState() {
        return memberState;
    }

    public void setMemberState(ByteBuffer memberState) {
        this.memberState = memberState;
    }

    @Override
    public int type() {
        return KafkaCommandKeys.SYNC_GROUP;
    }

    @Override
    public String toString() {
        StringBuilder responseStringBuilder = new StringBuilder();
        responseStringBuilder.append("Name: " + this.getClass().getSimpleName());
        return responseStringBuilder.toString();
    }
}
