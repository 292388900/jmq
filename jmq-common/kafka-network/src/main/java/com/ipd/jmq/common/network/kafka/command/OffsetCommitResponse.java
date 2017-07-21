package com.ipd.jmq.common.network.kafka.command;

import java.util.Map;

/**
 * Created by zhangkepeng on 16-8-4.
 */
public class OffsetCommitResponse implements KafkaRequestOrResponse{

    private int correlationId = 0;
    private Map<String, Map<Integer, Short>> commitStatus;

    public int getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(int correlationId) {
        this.correlationId = correlationId;
    }

    public Map<String, Map<Integer, Short>> getCommitStatus() {
        return commitStatus;
    }

    public void setCommitStatus(Map<String, Map<Integer, Short>> commitStatus) {
        this.commitStatus = commitStatus;
    }

    @Override
    public int type() {
        return KafkaCommandKeys.OFFSET_COMMIT;
    }
}
