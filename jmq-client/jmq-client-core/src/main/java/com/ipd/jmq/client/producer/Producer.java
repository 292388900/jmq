package com.ipd.jmq.client.producer;

import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.Message;
import com.ipd.jmq.toolkit.lang.LifeCycle;

import java.util.List;

/**
 * 生产者
 */
public interface Producer extends LifeCycle {
    /**
     * 非事务的单条发送
     *
     * @param message 消息
     * @throws com.ipd.jmq.common.exception.JMQException
     */
    void send(final Message message) throws JMQException;

    /**
     * 批量发送，不支持顺序消息，支持事务
     *
     * @param messages 消息列表
     * @throws com.ipd.jmq.common.exception.JMQException
     */
    void send(final List<Message> messages) throws JMQException;

    /**
     * 异步发送，不支持事务
     * @param message 消息
     * @param callback 回调函数
     * @throws JMQException
     */
    void sendAsync(final Message message, final AsyncSendCallback callback) throws JMQException;

    /**
     * 异步发送，不支持事务
     * @param messages 消息列表
     * @param callback 回调函数
     * @throws JMQException
     */
    void sendAsync(final List<Message> messages, final AsyncSendCallback callback) throws JMQException;

    /**
     * 开启事务
     * @param topic 主题
     * @param queryId 查询ID
     * @param txTimeout 事务超时时间
     * @throws JMQException
     */
    String beginTransaction(final String topic, final String queryId, final int txTimeout) throws JMQException;

    /**
     * 回滚事务
     *
     * @throws JMQException
     */
    void rollbackTransaction() throws JMQException;

    /**
     * 提交事务
     * @throws JMQException
     */
    void commitTransaction() throws JMQException;

}