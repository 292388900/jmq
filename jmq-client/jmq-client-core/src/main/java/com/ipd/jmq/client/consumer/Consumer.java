package com.ipd.jmq.client.consumer;

import com.ipd.jmq.toolkit.lang.LifeCycle;
import com.ipd.jmq.common.model.ConsumerConfig;


/**
 * 消费接口
 *
 * @author lindeqiang
 */
public interface Consumer extends LifeCycle {
    /**
     * 暂停全部消费
     */
    void pause();

    /**
     * 暂停指定主题的消费
     *
     * @param topic 主题
     */
    void pause(String topic);

    /**
     * 是否暂停
     *
     * @param topic 主题
     * @return true if paused
     */
    boolean isPaused(String topic);

    /**
     * 恢复全部暂停的消费
     */
    void resume();

    /**
     * 恢复指定主题的消费
     *
     * @param topic 主题
     */
    void resume(String topic);

    /**
     * 添加订阅，监听器模式
     *
     * @param topic    主题
     * @param listener 监听器
     */
    void subscribe(String topic, MessageListener listener);

    /**
     * 添加订阅，兼容器模式
     *
     * @param topic    主题
     * @param listener 监听器
     * @param config   消费配置
     */
    void subscribe(String topic, MessageListener listener, ConsumerConfig config);

    /**
     * 取消订阅，监听器模式
     *
     * @param topic 主题
     */
    void unSubscribe(String topic);

}