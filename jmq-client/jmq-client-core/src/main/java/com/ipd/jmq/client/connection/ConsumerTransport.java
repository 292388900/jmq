package com.ipd.jmq.client.connection;

import com.ipd.jmq.common.network.v3.session.ConsumerId;

/**
 * 消费者连接接口
 */
public interface ConsumerTransport extends GroupTransport {

    /**
     * 返回消费者ID
     *
     * @return 消费者ID
     */
    ConsumerId getConsumerId();

    /**
     * 设置选择器
     *
     * @param selector 选择器
     */
    void setSelector(String selector);

}
