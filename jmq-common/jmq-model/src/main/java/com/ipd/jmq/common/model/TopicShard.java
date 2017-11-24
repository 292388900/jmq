package com.ipd.jmq.common.model;

/**
 *
 */
public class TopicShard extends BaseModel {
    private Identity topic;
    private Identity shard;
    private String queues;

    public String getQueues() {
        return queues;
    }

    public void setQueues(String queues) {
        this.queues = queues;
    }

    public Identity getTopic() {
        return topic;
    }

    public void setTopic(Identity topic) {
        this.topic = topic;
    }

    public Identity getShard() {
        return shard;
    }

    public void setShard(Identity shard) {
        this.shard = shard;
    }
}
