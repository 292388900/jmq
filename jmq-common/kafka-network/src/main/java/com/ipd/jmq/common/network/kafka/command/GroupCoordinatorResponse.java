package com.ipd.jmq.common.network.kafka.command;

import com.ipd.jmq.common.network.kafka.model.KafkaBroker;

/**
 * Created by zhangkepeng on 17-2-9.
 */
public class GroupCoordinatorResponse implements KafkaRequestOrResponse {

    private int correlationId;
    private short errorCode;
    private KafkaBroker kafkaBroker;

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

    public KafkaBroker getKafkaBroker() {
        return kafkaBroker;
    }

    public void setKafkaBroker(KafkaBroker kafkaBroker) {
        this.kafkaBroker = kafkaBroker;
    }

    @Override
    public int type() {
        return KafkaCommandKeys.GROUP_COORDINATOR;
    }

    @Override
    public String toString() {
        StringBuilder responseStringBuilder = new StringBuilder();
        responseStringBuilder.append("Name: " + this.getClass().getSimpleName());
        return responseStringBuilder.toString();
    }
}
