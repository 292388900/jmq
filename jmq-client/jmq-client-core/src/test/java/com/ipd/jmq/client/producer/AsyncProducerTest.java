package com.ipd.jmq.client.producer;

import com.ipd.jmq.client.connection.ClusterTransportManager;
import com.ipd.jmq.client.connection.TransportConfig;
import com.ipd.jmq.client.connection.TransportManager;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.Message;
import com.ipd.jmq.common.network.v3.command.PutMessage;
import com.ipd.jmq.common.network.v3.command.Command;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by zhangkepeng on 15-11-17.
 */
public class AsyncProducerTest {
    private static final Logger logger = Logger.getLogger(AsyncProducerTest.class);
    private TransportManager manager;
    private MessageProducer producer;
    private String topic;
    private String app;
    private int sendTimeout;

    @Before
    public void setUp() throws Exception {
        app = "broad";
        topic = "broadcastTopic";
        sendTimeout = 5000;

        TransportConfig config = new TransportConfig();
        config.setApp(app);

        //TODO 设置broker地址
        config.setAddress("10.12.167.40:50088");
        config.setUser("jmq");
        config.setPassword("jmq");
        config.setSendTimeout(sendTimeout);

        //TODO Linux环境下设置为true
        config.setEpoll(true);

        manager = new ClusterTransportManager(config);
        manager.start();
        producer = new MessageProducer();
        producer.setTransportManager(manager);
        producer.start();
        Thread.sleep(1000);
    }

    @After
    public void terDown() throws Exception {
        if (producer != null) {
            producer.stop();
        }
        if (manager != null) {
            manager.stop();
        }
    }

    /**
     * 异步发送测试
     *
     * @throws JMQException
     */
    @Test
    public void testAsyncSend() throws JMQException {
        for (int i = 0; ; i++) {
            try {
                Message message = new Message(topic, "消息内容"+i, "业务ID"+i);
                producer.sendAsync(message, new AsyncSendCallback() {
                    @Override
                    public void success(PutMessage putMessage, Command response) {
                        logger.info("成功发送一条消息！！"+putMessage.getMessages()[0].getBusinessId());
                    }

                    @Override
                    public void exception(PutMessage putMessage, Throwable cause) {
                        logger.info("err一条消息！！"+putMessage.getMessages()[0].getBusinessId());
                    }
                });
                logger.info("发送一条消息！！"+i);
                Thread.sleep(500);
            } catch (Exception e) {
                e.printStackTrace();
                try {
                    Thread.sleep(2000);
                }   catch (Exception e1){

                }
            }
        }
    }
}
