package com.ipd.jmq.common.network.kafka.model;

import java.util.List;

/**
 * Created by zhangkepeng on 16-7-29.
 */
public class TopicMetadata {

    private String topic;
    private short errorCode;
    private List<PartitionMetadata> partitionMetadatas;

    public TopicMetadata(String topic, List<PartitionMetadata> partitionMetadatas, short errorCode) {
        this.topic = topic;
        this.errorCode = errorCode;
        this.partitionMetadatas = partitionMetadatas;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public short getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(short errorCode) {
        this.errorCode = errorCode;
    }

    public List<PartitionMetadata> getPartitionMetadatas() {
        return partitionMetadatas;
    }

    public void setPartitionMetadatas(List<PartitionMetadata> partitionMetadatas) {
        this.partitionMetadatas = partitionMetadatas;
    }
}
