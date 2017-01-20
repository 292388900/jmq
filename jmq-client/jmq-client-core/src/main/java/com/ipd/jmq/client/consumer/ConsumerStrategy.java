package com.ipd.jmq.client.consumer;

import com.ipd.jmq.common.cluster.TopicConfig;

/**
 * Created by dingjun on 15-10-27.
 */
public interface ConsumerStrategy {
    /**
     * 选择消费的组
     * @param topicConfig 主题配置
     * @param dataCenter 数据中心
     * @return jmq group
     */
    String electBrokerGroup(TopicConfig topicConfig, byte dataCenter);

    /**
     * 选择消费队列 0表示由服务端选择
     * @param groupName jmq group
     * @param topicConfig 主题配置
     * @param dataCenter 数据中心
     * @return Queue Id
     */
    short electQueueId(String groupName, TopicConfig topicConfig, byte dataCenter);

    /**
     * 拉取结束
     * @param groupName jmq group
     * @param queueId Queue Id
     */
    void pullEnd(String groupName, short queueId);
}
