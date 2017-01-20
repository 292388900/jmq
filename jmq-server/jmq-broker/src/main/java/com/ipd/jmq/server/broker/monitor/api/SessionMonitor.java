package com.ipd.jmq.server.broker.monitor.api;

import com.ipd.jmq.common.lb.TopicSession;
import com.ipd.jmq.common.monitor.Client;

import java.util.List;

/**
 * Created by zhangkepeng on 16-11-30.
 */
public interface SessionMonitor {

    /**
     * 获取broker上所有主题会话信息
     * @return
     */
    List<TopicSession> getTopicSessionsOnBroker();

    /**
     * 获得broker上的所有连接信息
     *
     * @return 连接列表
     */
    List<Client> getConnections();

    /**
     * 获取连接
     *
     * @param topic 主题
     * @param app   应用
     * @return 连接列表
     */
    List<Client> getConnections(String topic, String app);

    /**
     * 关闭生产者连接
     *
     * @param topic 应用
     * @param app   主题
     */
    void closeProducer(String topic, String app);

    /**
     * 关闭消费者连接
     *
     * @param topic 主题
     * @param app   应用
     */
    void closeConsumer(String topic, String app);
}
