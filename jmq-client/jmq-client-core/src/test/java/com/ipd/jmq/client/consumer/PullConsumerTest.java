package com.ipd.jmq.client.consumer;

import com.ipd.jmq.client.connection.ClusterTransportManager;
import com.ipd.jmq.client.connection.TransportConfig;
import com.ipd.jmq.client.connection.TransportManager;
import com.ipd.jmq.common.model.ConsumerConfig;
import com.ipd.jmq.common.message.Message;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 消费测试.
 */

public class PullConsumerTest {
    //连接管理器
    private TransportManager manager;
    //消息消费者，同一个App可以使用同一个消费者订阅多个主题
    private MessageConsumer messageConsumer;
    //消息处理器
    private MessageListener messageListener = new MessageListener() {
        @Override
        public void onMessage(List<Message> messages) throws Exception {
            System.out.println("--" + messages.size());
            if (messages != null) {
                for (Message message : messages) {
                    System.out.println("--" + message.getText());
                }
            }
        }
    };

    //多个消费者时，默认用第一个生成demo, 可以更换为其他消费者信息
//消费者app。 JMQ中的app等同于MQ中的systemId
    private String app = "bjllw";
    //消费者topic。JMQ中的topic等同于MQ中的destination
    private String topic = "bjllw";

    @Before
    public void setUp() throws Exception {

        TransportConfig config = new TransportConfig();
        config.setApp(app);
//设置broker地址
        config.setAddress("127.0.0.1:50088");
//设置用户名
        config.setUser("mq");
//设置密码
        config.setPassword("mq");
//设置发送超时
        config.setSendTimeout(20000);
        config.setConnectionTimeout(20000);
        config.setSoTimeout(20000);
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

        messageConsumer.subscribe(topic, messageListener);

        CountDownLatch latch = new CountDownLatch(1);
        latch.await();
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