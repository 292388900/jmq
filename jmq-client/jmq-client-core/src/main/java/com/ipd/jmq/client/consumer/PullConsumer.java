package com.ipd.jmq.client.consumer;


import com.ipd.jmq.toolkit.lang.LifeCycle;

/**
 * PullConsumer
 *
 * @author luoruiheng
 * @since 12/23/16
 */
public interface PullConsumer extends LifeCycle {

    /**
     * 拉取单条消息，拉模式
     *
     * @param topic    主题
     * @param listener 监听器
     * @return 实际拉取的数量
     */
    int pull(String topic, MessageListener listener);

    /**
     * 拉取消息
     *
     * @param topic 主题
     * @return 拉取结果接口(可确认或重试)
     */
    PullResult pull(String topic);



}
