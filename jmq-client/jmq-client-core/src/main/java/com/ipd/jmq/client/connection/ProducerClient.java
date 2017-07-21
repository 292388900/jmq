package com.ipd.jmq.client.connection;

import com.ipd.jmq.common.cluster.Permission;
import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.network.v3.command.AddProducer;
import com.ipd.jmq.common.network.v3.command.GetProducerHealth;
import com.ipd.jmq.common.network.v3.command.JMQHeader;
import com.ipd.jmq.common.network.v3.session.ProducerId;
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
 * 生产者连接实现
 */
public class ProducerClient extends GroupClient implements ProducerTransport {
    private static Logger logger = LoggerFactory.getLogger(ProducerClient.class);

    private ProducerId producerId;


    public ProducerClient(Client client, FailoverPolicy failoverPolicy, RetryPolicy retryPolicy) {
        super(client, failoverPolicy, retryPolicy);
        this.permission = Permission.WRITE;
    }

    @Override
    protected void validate() throws Exception {
        super.validate();

    }

    @Override
    protected boolean login(Transport transport) throws TransportException {
        boolean result = super.login(transport);
        if (result) {
            ProducerId newId = new ProducerId(connectionId);
            //发送创建生产者命令
            AddProducer addProducer = new AddProducer().producerId(newId).topic(topic);
            Command request = new Command(JMQHeader.Builder.request(addProducer.type(), Acknowledge.ACK_RECEIVE), addProducer);
            Command addAck = transport.sync(request, config.getSendTimeout());
            JMQHeader header = (JMQHeader) addAck.getHeader();
            result = header.getStatus() == JMQCode.SUCCESS.getCode();
            if (result) {
                // 创建成功，设置生产者ID
                producerId = newId;
            } else {
                logger.error(header.getError());
            }
        }
        return result;
    }

    @Override
    protected boolean checkHealth(Transport transport) throws TransportException {
        GetProducerHealth getHealth =
                new GetProducerHealth().topic(topic).app(config.getApp()).producerId(producerId.getProducerId())
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
    public ProducerId getProducerId() {
        return producerId;
    }

}
