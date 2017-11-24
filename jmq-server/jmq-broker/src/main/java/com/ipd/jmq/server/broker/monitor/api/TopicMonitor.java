package com.ipd.jmq.server.broker.monitor.api;

/**
 * Created by zhangkepeng on 16-11-30.
 */
public interface TopicMonitor {

    /**
     * 增加主题
     *
     * @param topic      主题
     * @param queueCount 普通队列数
     * @throws com.ipd.jmq.common.exception.JMQException
     */
    void addTopic(String topic, short queueCount);

    /**
     * 获取客户端性能指标
     *
     * @return
     */
    String collectMetrics();
}
