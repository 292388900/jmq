package com.ipd.jmq.client.consumer;

import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.BrokerMessage;
import com.ipd.jmq.common.message.Message;
import com.ipd.jmq.common.message.MessageLocation;
import java.util.ArrayList;
import java.util.List;

/**
 * PullResultHandler
 *
 * @author luoruiheng
 * @since 12/23/16
 */
public class PullResultHandler implements PullResult {

    private TopicConsumer topicConsumer;

    private String topic;
    private String consumerId;
    private String brokerGroup;
    private MessageLocation[] locations;

    public PullResultHandler(TopicConsumer topicConsumer) {
        this.topicConsumer = topicConsumer;
    }

    @Override
    public List<Message> getMessages() throws JMQException {
        if (null == topicConsumer) {
            throw new JMQException("consumer is null! ", JMQCode.CN_CONNECTION_ERROR.getCode());
        }
        // messageDispatches could not be null, so null-check not needed here
        List<MessageDispatch> messageDispatches = topicConsumer.pull();

        int index = 0;
        this.locations = new MessageLocation[messageDispatches.size()];
        List<Message> messages = new ArrayList<Message>();
        for (MessageDispatch dispatch : messageDispatches) {
            if (index == 0) {
                this.topic = dispatch.getMessage().getTopic();
                this.consumerId = dispatch.getConsumerId().getConsumerId();
                this.brokerGroup = dispatch.getGroup().getGroup();
            }
            BrokerMessage message = dispatch.getMessage();
            this.locations[index++] = new MessageLocation(message);
            messages.add(message);
        }

        return messages;
    }

    @Override
    public void acknowledge() throws JMQException {
        if (null == this.locations) {
            throw new JMQException("No message pulled! Have you called getMessages()?", JMQCode.CT_MESSAGE_BODY_NULL.getCode());
        }

        if (topicConsumer != null) {
            topicConsumer.acknowledge(consumerId, brokerGroup, locations, true);

            this.topic = null;
            this.consumerId = null;
            this.brokerGroup = null;
            this.locations = null;
        } else {
            throw new JMQException("consumer is null,consumerId:" + consumerId + ",group:" + brokerGroup + ",topic:" + topic, JMQCode.CN_CONNECTION_ERROR.getCode());
        }
    }

    @Override
    public void retry(String exception) throws JMQException {
        if (null == this.locations) {
            throw new JMQException("No message pulled! Have you called getMessages()?", JMQCode.CT_MESSAGE_BODY_NULL.getCode());
        }

        if (topicConsumer != null) {
            topicConsumer.retry(consumerId, brokerGroup, locations, exception);

            this.topic = null;
            this.consumerId = null;
            this.brokerGroup = null;
            this.locations = null;
        } else {
            throw new JMQException("consumer is null,consumerId:" + consumerId + ",group:" + brokerGroup + ",topic:" + topic, JMQCode.CN_CONNECTION_ERROR.getCode());
        }
    }
}
