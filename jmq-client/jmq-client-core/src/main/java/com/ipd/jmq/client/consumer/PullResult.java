package com.ipd.jmq.client.consumer;

import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.Message;

import java.util.List;

/**
 * PullResult
 *
 * @author luoruiheng
 * @since 12/23/16
 */
public interface PullResult {

    /**
     * 获取消息
     *
     * @return 消息列表
     * @throws JMQException
     */
    List<Message> getMessages() throws JMQException;

    /**
     * 确认消息
     *
     * @throws JMQException
     */
    void acknowledge() throws JMQException;

    /**
     * 重试消息
     *
     * @param exception 异常信息
     * @throws JMQException
     */
    void retry(String exception) throws JMQException;

}
