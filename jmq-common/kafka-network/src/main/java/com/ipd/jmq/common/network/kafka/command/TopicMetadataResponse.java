package com.ipd.jmq.common.network.kafka.command;

import com.ipd.jmq.common.network.kafka.model.KafkaBroker;
import com.ipd.jmq.common.network.kafka.model.TopicMetadata;

import java.util.List;
import java.util.Set;

/**
 * Created by zhangkepeng on 16-7-29.
 */
public class TopicMetadataResponse implements KafkaRequestOrResponse{

    private List<TopicMetadata> topicMetadatas;
    private Set<KafkaBroker> kafkaBrokers;
    private int correlationId;

    public TopicMetadataResponse(Set<KafkaBroker> kafkaBrokers, List<TopicMetadata> topicMetadatas, int correlationId) {
        this.kafkaBrokers = kafkaBrokers;
        this.topicMetadatas = topicMetadatas;
        this.correlationId = correlationId;
    }

    public List<TopicMetadata> getTopicMetadatas() {
        return topicMetadatas;
    }

    public void setTopicMetadatas(List<TopicMetadata> topicMetadatas) {
        this.topicMetadatas = topicMetadatas;
    }

    public Set<KafkaBroker> getKafkaBrokers() {
        return kafkaBrokers;
    }

    public void setKafkaBrokers(Set<KafkaBroker> kafkaBrokers) {
        this.kafkaBrokers = kafkaBrokers;
    }

    public int getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(int correlationId) {
        this.correlationId = correlationId;
    }

    @Override
    public int type() {
        return KafkaCommandKeys.METADATA;
    }

    @Override
    public String toString() {
        StringBuilder responseStringBuilder = new StringBuilder();
        responseStringBuilder.append("Name: " + this.getClass().getSimpleName());
        return responseStringBuilder.toString();
    }
}
