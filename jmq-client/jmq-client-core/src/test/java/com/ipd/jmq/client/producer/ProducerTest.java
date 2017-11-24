package com.ipd.jmq.client.producer;

import com.ipd.jmq.client.connection.ClusterTransportManager;
import com.ipd.jmq.client.connection.TransportConfig;
import com.ipd.jmq.client.connection.TransportManager;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.Message;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * 生产测试
 *
 * @author lindeqiang
 * @since 14-6-3 下午3:02
 */
public class ProducerTest {
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
        //config.setAddress("192.168.209.78:50088");
        config.setAddress("10.12.122.58:50088");
        config.setUser("jmq");
        config.setPassword("jmq");
        config.setSendTimeout(sendTimeout);

        //TODO Linux环境下设置为true
        config.setEpoll(false);

        TransportManager manager = new ClusterTransportManager(config);
        manager.start();
        producer = new MessageProducer();
        producer.setTransportManager(manager);
        producer.start();
        Thread.sleep(1000);
    }

    @After
    public void terDown() throws Exception {
        producer.stop();
    }

    /**
     * 发送测试
     *
     * @throws JMQException
     */
    @Test
    public void testMessageProducer() throws JMQException {
        for (int i = 0; i < 10000000; i++) {
            try {
                send(i,1000);
                System.out.println("--send--"+i);
            } catch (Exception e) {
//                e.printStackTrace();
            }
        }
    }

    /**
     * 发送测试
     *
     * @throws JMQException
     */
    @Test
    public void removeProducer() throws JMQException {

        for (int i = 0; i < 10000000; i++) {

            try {
                app = "localProducer";
                topic = "test";
                sendTimeout = 500;

                TransportConfig config = new TransportConfig();
                config.setApp(app);


                config.setAddress("127.0.0.1:50088");
                config.setUser("jmq");
                config.setPassword("jmq");
                config.setSendTimeout(sendTimeout);


                config.setEpoll(false);

                TransportManager manager = new ClusterTransportManager(config);
                manager.start();
                producer = new MessageProducer();
                producer.setTransportManager(manager);
                producer.start();
                Thread.sleep(1000);


                Message message = new Message(topic, topic + "_test_" + i, topic + "_rid_" + i);
                producer.send(message);




                System.out.println("--send ok--");
            } catch (Exception e) {

            }
        }
    }

    /**
     * 非事务的单条发送
     */
    protected void send(int i) throws Exception {


    }

    /**
     * 非事务的单条发送
     */
    protected void send(int i, int timeout) throws JMQException {
        Message message = new Message(topic, topic + "_test_" + i, topic + "_rid_" + i);
        producer.send(message);
    }

    /**
     * 事务的单条发送
     */
    protected void sendTransaction(int i, int timeout) throws JMQException {
        /*Message message = new Message(topic, topic + "_test_" + i, topic + "_rid_" + i);

        Boolean ret = producer.send(message, new LocalTransaction<Boolean>() {
            @Override
            public Boolean execute() throws Exception {
                System.out.println("execute local transaction ok!");
                return true;
            }

            @Override
            public int getTimeout() {
                return 5000;
            }
        }, timeout);

        System.out.println("local transaction ret value:" + ret);*/
    }

    /**
     * 批量发送，不支持顺序消息，不支持事务
     */
    protected void batchSend(int i, int batchSize) throws JMQException {
        List<Message> messages = new ArrayList<Message>();
        for (int j = 0; j < batchSize; j++) {
            Message message = new Message(topic, topic + "_test_batch_" + i + "seq_" + j, topic + "_rid_" + i + j);
        }

        producer.send(messages);
    }

    /**
     * 批量发送，不支持顺序消息，不支持事务
     */
    protected void batchSend(int i, int batchSize, int timeout) throws JMQException {
        List<Message> messages = new ArrayList<Message>();
        for (int j = 0; j < batchSize; j++) {
            Message message = new Message(topic, topic + "_test_batch_" + i + "seq_" + j, topic + "_rid_" + i + j);
        }

        producer.send(messages);
    }
}
