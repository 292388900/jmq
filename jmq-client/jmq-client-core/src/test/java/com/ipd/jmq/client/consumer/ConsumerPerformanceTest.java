package com.ipd.jmq.client.consumer;

import com.ipd.jmq.client.connection.ClusterTransportManager;
import com.ipd.jmq.client.connection.TransportConfig;
import com.ipd.jmq.client.connection.TransportManager;
import com.ipd.jmq.common.model.ConsumerConfig;
import com.ipd.jmq.common.message.Message;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 消费测试.
 */

public class ConsumerPerformanceTest {


    //多个消费者时，默认用第一个生成demo, 可以更换为其他消费者信息
//消费者app。 JMQ中的app等同于MQ中的systemId
    private String app = "bjllw";


    private long start = 0;

    private AtomicLong bjllwTotal = new AtomicLong(0);
    private AtomicLong bjllw1Total = new AtomicLong(0);
    private AtomicLong bjllw2Total = new AtomicLong(0);

    @Before
    public void setUp() throws Exception {


    }


    @Test
    public void startConsume() throws Exception {


        start = System.currentTimeMillis();
        for (int i = 0; i < 9; i++) {
            bjllw();
            bjllw1();
            bjllw2();
        }

        CountDownLatch latch = new CountDownLatch(1);
        latch.await();
    }


    public void bjllw() throws Exception {


        //连接管理器
        TransportManager manager;
        //消息消费者，同一个App可以使用同一个消费者订阅多个主题
        MessageConsumer messageConsumer;

        String topic = "bjllw";

        TransportConfig config = new TransportConfig();
        config.setApp(app);
        config.setAddress("127.0.0.1:50088");
        config.setUser("mq");
        config.setPassword("mq");
        config.setSendTimeout(20000);
        config.setConnectionTimeout(20000);
        config.setSoTimeout(20000);
        config.setEpoll(true);
        ConsumerConfig consumerConfig = new ConsumerConfig();
        manager = new ClusterTransportManager(config);
        messageConsumer = new MessageConsumer(consumerConfig, manager, null);

        //启动消费者
        messageConsumer.start();

        messageConsumer.subscribe(topic, new MessageListener() {
            @Override
            public void onMessage(List<Message> list) throws Exception {
                long value = bjllwTotal.addAndGet(list.size());
                if (value > 99998) {
                    System.out.println(bjllwTotal + " bjllw--messages--used--" + (System.currentTimeMillis() - start));
                    for (Message message : list) {
                    }
                }

            }
        });

    }


    public void bjllw1() throws Exception {


        //连接管理器
        TransportManager manager;
        //消息消费者，同一个App可以使用同一个消费者订阅多个主题
        MessageConsumer messageConsumer;

        String topic = "bjllw1";

        TransportConfig config = new TransportConfig();
        config.setApp(app);
        config.setAddress("127.0.0.1:50088");
        config.setUser("mq");
        config.setPassword("mq");
        config.setSendTimeout(20000);
        config.setConnectionTimeout(20000);
        config.setSoTimeout(20000);
        config.setEpoll(true);
        ConsumerConfig consumerConfig = new ConsumerConfig();
        manager = new ClusterTransportManager(config);
        messageConsumer = new MessageConsumer(consumerConfig, manager, null);

        //启动消费者
        messageConsumer.start();

        messageConsumer.subscribe(topic, new MessageListener() {
            @Override
            public void onMessage(List<Message> list) throws Exception {

                long value = bjllw1Total.addAndGet(list.size());
                if (value > 99997) {
                    System.out.println(bjllw1Total + "--bjllw1--messages--used--" + (System.currentTimeMillis() - start));
                    for (Message message : list) {

                    }
                }

            }
        });


    }

    public void bjllw2() throws Exception {

        //连接管理器
        TransportManager manager;
        //消息消费者，同一个App可以使用同一个消费者订阅多个主题
        MessageConsumer messageConsumer;


        String topic = "bjllw2";

        TransportConfig config = new TransportConfig();
        config.setApp(app);
        config.setAddress("127.0.0.1:50088");
        config.setUser("mq");
        config.setPassword("mq");
        config.setSendTimeout(20000);
        config.setConnectionTimeout(20000);
        config.setSoTimeout(20000);
        config.setEpoll(true);
        ConsumerConfig consumerConfig = new ConsumerConfig();
        manager = new ClusterTransportManager(config);
        messageConsumer = new MessageConsumer(consumerConfig, manager, null);

        //启动消费者
        messageConsumer.start();

        messageConsumer.subscribe(topic, new MessageListener() {
            @Override
            public void onMessage(List<Message> list) throws Exception {

                long value = bjllw2Total.addAndGet(list.size());
                if (value > 99997) {
                    System.out.println(bjllw2Total + "--bjllw2--messages--used--" + (System.currentTimeMillis() - start));
                    for (Message message : list) {
                    }
                }
            }
        });

    }

}