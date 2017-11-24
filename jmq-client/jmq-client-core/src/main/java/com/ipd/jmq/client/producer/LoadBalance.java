package com.ipd.jmq.client.producer;

import com.ipd.jmq.common.network.v3.netty.failover.ElectTransport;
import com.ipd.jmq.common.cluster.TopicConfig;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.Message;

import java.util.List;


/**
 * 负载均衡
 *
 * @author lindeqiang
 * @since 14-5-4 上午10:36
 */
public interface LoadBalance {
    /**
     * 选取broker
     *
     * @param transports 分组列表
     * @param errTransports 发送异常的分组
     * @param message    消息 如果发送多条消息，取的第一条消息
     * @param dataCenter 当前客户端所在数据中心
     * @param config 主题配置
     * @return 指定的分组
     */
    <T extends ElectTransport> T electTransport(List<T> transports, List<T> errTransports, Message message, byte dataCenter, TopicConfig config) throws
            JMQException;

    /**
     * 选择queueId 0表示随机
     * @param transport 当前发送分组
     * @param message 消息 如果发送多条消息，取的第一条消息
     * @param dataCenter 当前客户端所在数据中心
     * @param config 主题配置
     * @return 选取的 QueueId
     * @throws JMQException
     */
    <T extends ElectTransport> short electQueueId(T transport, Message message, byte dataCenter, TopicConfig config) throws JMQException;
}