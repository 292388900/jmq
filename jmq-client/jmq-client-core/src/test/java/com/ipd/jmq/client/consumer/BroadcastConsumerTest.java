package com.ipd.jmq.client.consumer;

import com.ipd.jmq.client.connection.ClusterTransportManager;
import com.ipd.jmq.client.connection.TransportConfig;
import com.ipd.jmq.common.model.ConsumerConfig;
import com.ipd.jmq.client.connection.TransportManager;
import com.ipd.jmq.client.offset.FileOffsetManageTest;
import com.ipd.jmq.client.producer.MessageProducer;
import com.ipd.jmq.common.message.Message;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by zhangkepeng on 15-9-18.
 */
public class BroadcastConsumerTest {
    private static final Logger logger = Logger.getLogger(BroadcastConsumerTest.class);
    private TransportManager jmqManager;
    private TransportManager broadManager;
    private MessageConsumer jmqMessageConsumer;
    private MessageConsumer broadMessageConsumer;
    private FileOffsetManageTest fileOffsetManageTest;
    private AtomicLong jmqConsumerCount = new AtomicLong(1);
    private AtomicLong broadConsumerCount = new AtomicLong(1);
    private String topic = "broadcastTopic";
    private String jmqApp = "jmq";
    private String broadApp = "broad";
    private String address="10.12.167.40:50088";

    /**
     * 开启连接、本地偏移量管理
     */
    @Before
    public void setUp() throws Exception {
        TransportConfig configBroadApp = new TransportConfig();
        configBroadApp.setApp(broadApp);
        //设置broker地址
        configBroadApp.setAddress(address);
        //设置用户名
        configBroadApp.setUser("jmq");
        //设置密码
        configBroadApp.setPassword("jmq");
        //设置发送超时
        configBroadApp.setSendTimeout(10000);
        //设置是否使用epoll模式，windows环境下设置为false，linux环境下设置为true
        configBroadApp.setEpoll(true);
        //创建集群连接管理器
        broadManager = new ClusterTransportManager(configBroadApp);
        broadManager.start();

        TransportConfig configJmqApp = new TransportConfig();
        configJmqApp.setApp(jmqApp);
        //设置broker地址
        configJmqApp.setAddress(address);
        //设置用户名
        configJmqApp.setUser("jmq");
        //设置密码
        configJmqApp.setPassword("jmq");
        //设置发送超时
        configJmqApp.setSendTimeout(10000);
        //设置是否使用epoll模式，windows环境下设置为false，linux环境下设置为true
        configJmqApp.setEpoll(true);
        //创建集群连接管理器
        jmqManager = new ClusterTransportManager(configJmqApp);
        jmqManager.start();

        //本地偏移量管理初始化
        fileOffsetManageTest = new FileOffsetManageTest();
        fileOffsetManageTest.setUp();
    }

    /**
     * 关闭连接资源
     * @throws Exception
     */
    @After
    public void terDown() throws Exception {
        if (jmqManager != null) {
            jmqManager.stop();
        }
        if (broadManager != null) {
            broadManager.stop();
        }
        if (jmqMessageConsumer != null) {
            jmqMessageConsumer.stop();
        }
        if (broadMessageConsumer != null) {
            broadMessageConsumer.stop();
        }
    }

    @Test
    public void testBroadConsumer() {
        try {
            //监控本地偏移量大小，本地记录的偏移量是生产者发送的总条数，而不是消费者订阅开始的消费积压数
            monitorLocalOffsetCount(broadApp, "jmq6", topic, (short)5);
            monitorLocalOffsetCount(jmqApp, "jmq6", topic, (short)5);
            //同时启动两个或多个消费者，观察发送的同一条消息是否被同时消费
            testBroadAppConsumer();
            testJmqAppConsumer();
            //阻塞持续消费
            CountDownLatch latch = new CountDownLatch(1);
            latch.await();
        } catch (Exception e) {
            logger.info(e);
        }
    }

    @Test
    public void testBroadAppConsumer() throws Exception{
        ConsumerConfig consumerConfig = new ConsumerConfig();
        broadMessageConsumer = new MessageConsumer(consumerConfig, broadManager, null);

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    broadMessageConsumer.start();
                } catch (Exception e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }

                broadMessageConsumer.subscribe(topic, new MessageListener() {
                    @Override
                    public void onMessage(List<Message> messages) throws Exception {

                        if (messages != null && !messages.isEmpty()) {
                            for (Message msg : messages) {
                                logger.info(broadApp + " queueId of message is " + msg.getQueueId());
                                logger.info(broadApp + " consumer count is " + broadConsumerCount.getAndIncrement());
                            }
                        }

                    }
                });

            }
        }).start();
    }

    @Test
    public void testJmqAppConsumer() throws Exception{
        ConsumerConfig consumerConfig = new ConsumerConfig();
        jmqMessageConsumer = new MessageConsumer(consumerConfig, jmqManager, null);

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    jmqMessageConsumer.start();
                } catch (Exception e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }

                jmqMessageConsumer.subscribe(topic, new MessageListener() {
                    @Override
                    public void onMessage(List<Message> messages) throws Exception {

                        if (messages != null && !messages.isEmpty()) {
                            for (Message msg : messages) {
                                logger.info(jmqApp + " queueId of message is " + msg.getQueueId());
                                logger.info(jmqApp + " consumer count is " + jmqConsumerCount.getAndIncrement());
                            }
                        }

                    }
                });

            }
        }).start();
    }

    public void monitorLocalOffsetCount(final String clientName, final String group, final String topic, final short queues) throws Exception{

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        logger.info("clientName:" + clientName + " group:" + group + ", total message is " +  fileOffsetManageTest.testMessageCount(clientName, group, topic,queues));
                        Thread.sleep(2000L);
                    }
                } catch (Exception e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }).start();
    }
}
