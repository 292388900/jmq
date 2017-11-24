package com.ipd.jmq.common.network.kafka.command;

import com.ipd.jmq.common.network.kafka.model.FetchResponsePartitionData;

import java.util.Map;

/**
 * Created by zhangkepeng on 16-8-4.
 */
public class FetchResponse implements KafkaRequestOrResponse {

    private int correlationId;
    private int throttleTimeMs = -1;
    private Map<String, Map<Integer, FetchResponsePartitionData>> fetchResponses;

    public int getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(int correlationId) {
        this.correlationId = correlationId;
    }

    public Map<String, Map<Integer, FetchResponsePartitionData>> getFetchResponses() {
        return fetchResponses;
    }

    public void setFetchResponses(Map<String, Map<Integer, FetchResponsePartitionData>> fetchResponses) {
        this.fetchResponses = fetchResponses;
    }

    public int getThrottleTimeMs() {
        return throttleTimeMs;
    }

    public void setThrottleTimeMs(int throttleTimeMs) {
        this.throttleTimeMs = throttleTimeMs;
    }

    @Override
    public int type() {
        return KafkaCommandKeys.FETCH;
    }

    @Override
    public String toString() {
        StringBuilder responseStringBuilder = new StringBuilder();
        responseStringBuilder.append("Name: " + this.getClass().getSimpleName());
        responseStringBuilder.append("fetchResponses: " + fetchResponses);
        return responseStringBuilder.toString();
    }
}
