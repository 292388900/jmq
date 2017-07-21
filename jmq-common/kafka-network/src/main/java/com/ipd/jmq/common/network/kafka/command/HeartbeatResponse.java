package com.ipd.jmq.common.network.kafka.command;

/**
 * Created by zhangkepeng on 17-2-10.
 */
public class HeartbeatResponse implements KafkaRequestOrResponse {

    private int correlationId;

    private short errorCode;

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

    @Override
    public int type() {
        return KafkaCommandKeys.HEARTBEAT;
    }

    @Override
    public String toString() {
        StringBuilder responseStringBuilder = new StringBuilder();
        responseStringBuilder.append("Name: " + this.getClass().getSimpleName());
        return responseStringBuilder.toString();
    }
}
