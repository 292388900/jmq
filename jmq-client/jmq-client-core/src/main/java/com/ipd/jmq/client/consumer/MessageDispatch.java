package com.ipd.jmq.client.consumer;

import com.ipd.jmq.common.cluster.BrokerGroup;
import com.ipd.jmq.common.message.BrokerMessage;
import com.ipd.jmq.common.network.v3.session.ConsumerId;

/**
 * 已派发消息
 *
 * @author lindeqiang
 * @since 14-5-7 下午6:37
 */
public class MessageDispatch {
    // 消息所属分组
    private BrokerGroup group;
    // 消费者Id
    private ConsumerId consumerId;
    // 消息
    private BrokerMessage message;

    public MessageDispatch(BrokerMessage message, ConsumerId consumerId, BrokerGroup group) {
        this.message = message;
        this.consumerId = consumerId;
        this.group = group;
    }

    public ConsumerId getConsumerId() {
        return consumerId;
    }

    public BrokerGroup getGroup() {
        return group;
    }

    public BrokerMessage getMessage() {
        return message;
    }

}
