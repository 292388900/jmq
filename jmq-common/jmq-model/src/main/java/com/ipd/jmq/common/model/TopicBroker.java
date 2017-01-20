package com.ipd.jmq.common.model;

/**
 * 主题和Broker的映射对象
 */
public class TopicBroker extends BaseModel {
    private long topicId;
    private String brokerGroup;

    public long getTopicId() {
        return topicId;
    }

    public void setTopicId(long topicId) {
        this.topicId = topicId;
    }

    public String getBrokerGroup() {
        return brokerGroup;
    }

    public void setBrokerGroup(String brokerGroup) {
        this.brokerGroup = brokerGroup;
    }
}
