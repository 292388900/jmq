package com.ipd.jmq.demo.producer;

import com.ipd.jmq.client.connection.ClusterTransportManager;
import com.ipd.jmq.client.connection.TransportConfig;
import com.ipd.jmq.client.connection.TransportManager;
import com.ipd.jmq.client.producer.MessageProducer;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.Message;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * 生产测试
 */
public class ApiProducerTest {
    private static final Logger logger = Logger.getLogger(ApiProducerTest.class);
    private TransportManager manager;
    private MessageProducer producer;
    // @Value("${jmq.producer.topic}")
    private String topic = "topic_simple";
    // @Value("${jmq.producer.app}")
    private String app = "app4Product";
    //  @Value("${jmq.address}")
//    private String address="10.12.128.36:50088";
//    private String address = "10.42.0.141:50088";
    private String address = "192.168.1.5:50088";

    @Before
    public void setUp() throws Exception {
        //连接配置
        TransportConfig config = new TransportConfig();
        config.setApp(app);
        //设置broker地址
        config.setAddress(address);
        //设置用户名
        config.setUser("jmq");
        //设置密码
        config.setPassword("jmq");
        //设置发送超时
        config.setSendTimeout(10000);
        //设置是否使用epoll模式，windows环境下设置为false，linux环境下设置为true
        config.setEpoll(false);

        //创建集群连接管理器
        manager = new ClusterTransportManager(config);
        manager.start();

        //创建发送者
        producer = new MessageProducer(manager);
        producer.start();

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
     * 发送测试
     *
     * @throws com.ipd.jmq.common.exception.JMQException
     */
    @Test
    public void testMessageProducer() throws JMQException {
        for (int i = 0; i<30; i++) {
            try {
                //sendSequential(i);
                send(i);
                //sendCompressedMsg(i);
                logger.info("成功发送一条消息！！");
                  Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
                try {
                    Thread.sleep(2000);
                } catch (Exception ignored) {

                }
            }
        }
    }


    private void sendCompressedMsg(int i) throws JMQException {
        Message message = new Message(topic,
                "compressed text_shfgjklafjglkjgsklfjglkjfsklgjoifgjlkafjgklajgkljajgklajfgkljaiogjrileajhglkajhlkjakljhoiajhoiajhiajhlkajklgajklhgjaklhjaklhjlkahjklahjkalhjklajhklsdfjhoiadfjhoiasfjhoithfahafdhaa" + i,
                "bzID_" + i,
                Message.CompressionType.Snappy);
        producer.send(message);
    }

    /**
     * 非事务的单条发送
     */
    protected void send(int i) throws JMQException {
        Message message = new Message(topic, "" + i, "业务ID" + i);
        producer.send(message);
    }


    /**
     * 非事务的单条发送
     */
    protected void sendSequential(int i) throws JMQException {
        Message message = new Message(topic, "消息内容" + i, "业务ID" + i);
        message.setOrdered(true);
        producer.send(message);
    }

    /**
     * 批量发送，不支持顺序消息，不支持事务
     */
    protected void batchSend(int i, int batchSize) throws JMQException {
        List<Message> messages = new ArrayList<Message>();
        for (int j = 0; j < batchSize; j++) {
            Message message = new Message(topic, "消息内容" + i, "业务ID" + i);
            messages.add(message);
        }

        producer.send(messages);
    }

}
