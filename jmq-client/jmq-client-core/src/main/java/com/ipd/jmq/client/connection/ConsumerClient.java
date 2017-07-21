package com.ipd.jmq.client.connection;

import com.ipd.jmq.common.cluster.Permission;
import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.network.v3.command.AddConsumer;
import com.ipd.jmq.common.network.v3.command.GetConsumerHealth;
import com.ipd.jmq.common.network.v3.command.JMQHeader;
import com.ipd.jmq.common.network.v3.session.ConsumerId;
import com.ipd.jmq.common.network.Client;
import com.ipd.jmq.common.network.FailoverPolicy;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.model.Acknowledge;
import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.toolkit.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 消费者连接
 */
public class ConsumerClient extends GroupClient implements ConsumerTransport {
    private static Logger logger = LoggerFactory.getLogger(ConsumerClient.class);

    private ConsumerId consumerId;

    // 选择器
    private String selector;


    public ConsumerClient(Client client, FailoverPolicy failoverPolicy, RetryPolicy retryPolicy) {
        super(client, failoverPolicy, retryPolicy);
        this.permission = Permission.READ;
    }

    @Override
    protected void validate() throws Exception {
        super.validate();

    }

    @Override
    protected boolean login(Transport transport) throws TransportException {
        boolean result = super.login(transport);
        if (result) {
            ConsumerId newId = new ConsumerId(connectionId);
            //发送创建消费者命令
            AddConsumer addConsumer = new AddConsumer().consumerId(newId).topic(topic).selector(selector);

            Command request = new Command(JMQHeader.Builder.request(addConsumer.type(), Acknowledge.ACK_RECEIVE), addConsumer);

            Command addAck = transport.sync(request);
            JMQHeader header = (JMQHeader) addAck.getHeader();
            result = header.getStatus() == JMQCode.SUCCESS.getCode();
            if (result) {
                consumerId = newId;
            } else {
                logger.error(header.getError());
            }
        }
        return result;
    }

    @Override
    protected boolean checkHealth(Transport transport) throws TransportException {
        GetConsumerHealth getHealth =
                new GetConsumerHealth().topic(topic).app(config.getApp()).consumerId(consumerId.getConsumerId())
                        .dataCenter(dataCenter);

        Command request = new Command(JMQHeader.Builder.request(getHealth.type(), Acknowledge.ACK_RECEIVE), getHealth);

        Command response = transport.sync(request, config.getSendTimeout());
        JMQHeader header = (JMQHeader) response.getHeader();
        if (header.getStatus() != JMQCode.SUCCESS.getCode()) {
            logger.error(header.getError());
            return false;
        }
        return true;
    }

    @Override
    public ConsumerId getConsumerId() {
        return consumerId;
    }

    @Override
    public void setSelector(String selector) {
        this.selector = selector;
    }
}
