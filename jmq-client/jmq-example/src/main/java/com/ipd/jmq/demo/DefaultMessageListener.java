package com.ipd.jmq.demo;

import com.ipd.jmq.client.consumer.MessageListener;
import com.ipd.jmq.common.message.Message;
import org.apache.log4j.Logger;


import java.util.List;

/**
 * 消息监听器.
 */
public class DefaultMessageListener implements MessageListener {
    private static final Logger logger = Logger.getLogger("consume");

    /**
     * 消费方法。注意: 消费不成功请抛出异常，MQ会自动重试
     *
     * @param messages
     * @throws Exception
     */
    @Override
    public void onMessage(List<Message> messages) throws Exception {
        if (messages == null || messages.isEmpty()) {
            return;
        }

        for (Message message : messages) {
            logger.info(String.format("收到一条消息,消息主题（队列名）：%s,内容是：%s", message.getTopic(), message.getText()));
        }
    }
}
