package com.ipd.jmq.server.broker.cluster;

import com.ipd.jmq.common.cluster.TopicConfig;

/**
 * 主题修改通知时间
 */
public class TopicUpdateEvent extends ClusterEvent {

    private TopicConfig topicConfig;

    public TopicUpdateEvent(TopicConfig topicConfig) {
        this.topicConfig = topicConfig;
        this.type = EventType.TOPIC_UPDATE;
    }

    public TopicConfig getTopicConfig() {
        return topicConfig;
    }

}
