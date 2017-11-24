package com.ipd.jmq.client.producer;

import com.ipd.jmq.common.cluster.TopicConfig;
import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.Message;
import com.ipd.jmq.common.network.v3.netty.failover.ElectTransport;
import com.ipd.jmq.common.network.FailoverState;
import com.ipd.jmq.common.network.Loadbalances;

import java.util.ArrayList;
import java.util.List;

/**
 * 权重负载均衡
 */
public class WeightLoadBalance implements LoadBalance {
    public static final short SEQUENTIAL_QUEUE_ID = 1;

    @Override
    public <T extends ElectTransport> T electTransport(final List<T> transports, final List<T> errTransports, final Message message,
                                                       final byte dataCenter, final TopicConfig config) throws JMQException {
        if (transports == null) {
            return null;
        }
        int size = transports.size();
        if (size == 0) {
            return null;
        } else if (size == 1) {
            return transports.get(0);
        } else if (size > 1 && isStrictOrdered(config)) {
            // 严格顺序消息不允许有多组broker
            throw new JMQException(JMQCode.CT_SEQUENTIAL_BROKER_AMBIGUOUS.getCode());
        } else if (isOrdered(message)) {
            // 判断是否是顺序消息
            // 按照业务ID计算hashcode
            int hashCode = message.getBusinessId().hashCode();
            hashCode = hashCode > Integer.MIN_VALUE ? hashCode : Integer.MIN_VALUE + 1;
            int random = Math.abs(hashCode) % size;
            T transport = transports.get(random);
            if (transport.getState() == FailoverState.CONNECTED) {
                // 当前连接正常
                return transport;
            }
        }
        List<T> curTransports = null;
        if (errTransports != null && !errTransports.isEmpty()) {
            curTransports = new ArrayList<T>();
            curTransports.addAll(transports);
            curTransports.removeAll(errTransports);
            if (curTransports.isEmpty()) {
                curTransports = transports;
            }
        } else {
            curTransports = transports;
        }
        return Loadbalances.randomWeight(curTransports);
    }

    @Override
    public <T extends ElectTransport> short electQueueId(T transport, Message message, byte dataCenter, TopicConfig config) throws JMQException {
        if (config.checkSequential()) {
            return SEQUENTIAL_QUEUE_ID;
        } else {
            return 0;
        }
    }


    /**
     * 是否是顺序消息
     *
     * @param message 消息
     * @return
     */
    protected boolean isOrdered(final Message message) {
        return message != null && message.isOrdered() &&
                message.getBusinessId() != null && !message.getBusinessId().isEmpty();
    }

    /**
     * 是否是严格顺序消息
     *
     * @param config
     * @return
     */
    protected boolean isStrictOrdered(final TopicConfig config) {
        return config != null && config.checkSequential();
    }
}