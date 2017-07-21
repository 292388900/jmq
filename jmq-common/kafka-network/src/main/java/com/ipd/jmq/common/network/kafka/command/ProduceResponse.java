package com.ipd.jmq.common.network.kafka.command;

import com.ipd.jmq.common.network.kafka.model.ProducerPartitionStatus;

import java.util.List;
import java.util.Map;

/**
 * Created by zhangkepeng on 16-8-1.
 */
public class ProduceResponse implements KafkaRequestOrResponse {

    private Map<String, List<ProducerPartitionStatus>> producerResponseStatuss;
    private int correlationId;

    public int getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(int correlationId) {
        this.correlationId = correlationId;
    }

    public Map<String, List<ProducerPartitionStatus>> getProducerResponseStatuss() {
        return producerResponseStatuss;
    }

    public void setProducerResponseStatuss(Map<String, List<ProducerPartitionStatus>> producerResponseStatuss) {
        this.producerResponseStatuss = producerResponseStatuss;
    }

    @Override
    public int type() {
        return KafkaCommandKeys.PRODUCE;
    }
}
