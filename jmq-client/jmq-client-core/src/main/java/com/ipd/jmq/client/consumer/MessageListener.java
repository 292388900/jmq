package com.ipd.jmq.client.consumer;

import java.util.List;

import com.ipd.jmq.common.message.BrokerMessage;
import com.ipd.jmq.common.message.Message;

/**
 * 消息监听器接口
 */
public interface MessageListener {
    /**
     * 消息监听
     *
     * @param messages 消息列表
     * @throws Exception
     */
    void onMessage(List<Message> messages) throws Exception;
}