package com.ipd.jmq.demo.consumer;

import com.ipd.jmq.client.connection.ClusterTransportManager;
import com.ipd.jmq.client.connection.TransportConfig;
import com.ipd.jmq.client.connection.TransportManager;
import com.ipd.jmq.common.model.ConsumerConfig;
import com.ipd.jmq.client.consumer.MessageConsumer;
import com.ipd.jmq.client.consumer.MessageListener;
import com.ipd.jmq.demo.DefaultMessageListener;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;

import java.util.concurrent.CountDownLatch;

/**
 * 消费测试.
 */

public class ApiConsumerTest {
    private static final Logger logger = Logger.getLogger(ApiConsumerTest.class);
    //连接管理器
    private TransportManager manager;
    //消息消费者，同一个App可以使用同一个消费者订阅多个主题
    private MessageConsumer messageConsumer;
   //消息处理器
    private MessageListener messageListener = new DefaultMessageListener();

    //消费者app。 JMQ中的app等同于MQ中的systemId
    @Value("${jmq.consumer.app}")
    private String app = "app4Consumer";
    //消费者topic。JMQ中的topic等同于MQ中的destination
    @Value("${jmq.consumer.topic}")
    private String topic = "topic_simple";
    @Value("${jmq.address}")
    //
    private String address = "192.168.1.5:50088";

    @Before
    public void setUp() throws Exception {

        TransportConfig config = new TransportConfig();
        config.setApp(app);
        //设置broker地址
        config.setAddress(address);
        //设置用户名
        config.setUser("jmq");
        //设置密码
        config.setPassword("jmq");
        //设置发送超时
        config.setSendTimeout(5000);
        //设置是否使用epoll模式，windows环境下设置为false，linux环境下设置为true
        config.setEpoll(false);

        ConsumerConfig consumerConfig = new ConsumerConfig();
        manager = new ClusterTransportManager(config);
        messageConsumer = new MessageConsumer(consumerConfig, manager, null);
    }

    @Test
    public void testMessageConsumer() throws Exception {
        //启动消费者
        messageConsumer.start();
        //订阅主题
        messageConsumer.subscribe(topic, messageListener);

        CountDownLatch latch = new CountDownLatch(1);
        latch.await();
    }

    @Test
    public void testPullMessageConsumer() throws Exception{
        messageConsumer.start();
        for (; ; ) {
            //手动拉取消息
            messageConsumer.pull(topic, messageListener);
        }
    }



    @After
    public void terDown() throws Exception {
        if (messageConsumer != null) {
            messageConsumer.stop();
        }

        if (manager != null) {
            manager.stop();
        }

    }

}
