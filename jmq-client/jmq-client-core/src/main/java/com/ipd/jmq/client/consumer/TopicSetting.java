package com.ipd.jmq.client.consumer;

import com.ipd.jmq.common.model.ConsumerConfig;
/**
 * 主题配置.
 *
 * @author lindeqiang
 * @since 14-6-9 上午10:00
 */
public class TopicSetting {
    //主题
    private String topic;
    //过滤器
    private String selector;
    //监听器
    private MessageListener listener;
    //消费配置
    private ConsumerConfig config;

    public TopicSetting() {
    }

    public TopicSetting(String topic, String selector, MessageListener listener) {
      this(topic, selector, listener, null);
    }

    public TopicSetting(String topic, String selector, MessageListener listener, ConsumerConfig config) {
        if (topic == null || topic.isEmpty()) {
            throw new IllegalStateException("topic can not be null");
        }

        if (listener == null) {
            throw new IllegalStateException("listener can not be null");
        }
        this.topic = topic;
        this.selector = selector;
        this.listener = listener;
        this.config = config;
    }

    public String getSelector() {
        return selector;
    }

    public void setSelector(String selector) {
        this.selector = selector;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public MessageListener getListener() {
        return listener;
    }

    public void setListener(MessageListener listener) {
        this.listener = listener;
    }

    public ConsumerConfig getConfig() {
        return config;
    }

    public void setConfig(ConsumerConfig config) {
        this.config = config;
    }
}
