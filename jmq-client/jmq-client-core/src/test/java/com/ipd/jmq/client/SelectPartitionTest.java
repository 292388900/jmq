package com.ipd.jmq.client;

import com.ipd.jmq.client.connection.ClusterTransportManager;
import com.ipd.jmq.client.connection.TransportConfig;
import com.ipd.jmq.client.connection.TransportManager;
import com.ipd.jmq.common.model.ConsumerConfig;
import com.ipd.jmq.client.consumer.ConsumerStrategy;
import com.ipd.jmq.client.consumer.MessageConsumer;
import com.ipd.jmq.client.consumer.MessageListener;
import com.ipd.jmq.client.producer.LoadBalance;
import com.ipd.jmq.client.producer.MessageProducer;
import com.ipd.jmq.common.cluster.TopicConfig;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.Message;
import com.ipd.jmq.common.network.v3.netty.failover.ElectTransport;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 测试选择partition发送与消费，包含指定partition生产和消费以及非指定partition生产和消费
 *
 * Created by zhangkepeng on 15-11-18.
 */
public class SelectPartitionTest {
    private static final Logger logger = Logger.getLogger(SelectPartitionTest.class);
    private TransportManager manager;
    private MessageProducer producer;
    private MessageConsumer messageConsumer;
    private AtomicLong producerCount = new AtomicLong(1);
    private AtomicLong errProducerCount = new AtomicLong(1);
    private AtomicLong consumerCount = new AtomicLong(1);
    private String topic = "abc";
    private String app = "abc";
    private String address="10.12.167.47:50088";

    /**
     * 开启连接
     */
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
        config.setSendTimeout(10000);
        //设置是否使用epoll模式，windows环境下设置为false，linux环境下设置为true
        config.setEpoll(true);
        //创建集群连接管理器
        manager = new ClusterTransportManager(config);
        manager.start();
    }

    /**
     * 关闭连接资源
     * @throws Exception
     */
    @After
    public void terDown() throws Exception {
        if (manager != null) {
            manager.stop();
        }
        if (producer != null) {
            producer.stop();
        }
        if (messageConsumer != null) {
            messageConsumer.stop();
        }
    }

    /**
     * 测试指定partition消费
     *
     * @throws Exception
     */
    @Test
    public void testSelectPartitionProducer() throws Exception {
        //创建发送者
        producer = new MessageProducer(manager);
        producer.setLoadBalance(new ProducerLoadBalanceTest());
        producer.setTransportManager(manager);
        producer.start();
        Thread.sleep(3000);

        new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 100; i++) {
                    try{
                        send(i);
                        logger.info("producer count is " + producerCount.getAndIncrement());
                    }catch (Exception e){
                        e.printStackTrace();
                        logger.info("error count is " + errProducerCount.getAndIncrement());
                        System.out.println("error i = "+i);
                    }
                }
            }
        }).start();
        CountDownLatch latch = new CountDownLatch(1);
        latch.await();
    }

    /**
     * 远程与本地消费消息，远程与本地可以在消费策略上进行配置
     *
     * @throws Exception
     */
    @Test
    public void testConsumer() throws Exception{
        ConsumerConfig consumerConfig = new ConsumerConfig();
        messageConsumer = new MessageConsumer(consumerConfig, manager, null);

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    messageConsumer.start();
                } catch (Exception e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }

                messageConsumer.subscribe(topic, new MessageListener() {
                    @Override
                    public void onMessage(List<Message> messages) throws Exception {

                        if (messages != null && !messages.isEmpty()) {
                            for (Message msg : messages) {
                                logger.info("message queueId is " + msg.getQueueId());
                                logger.info("consumer count is " + consumerCount.getAndIncrement());
                            }
                        }

                    }
                });

            }
        }).start();
        CountDownLatch latch = new CountDownLatch(1);
        latch.await();
    }

    /**
     * 发送消息
     *
     * @throws Exception
     */
    @Test
    public void testProducer() throws Exception {
        //创建发送者
        producer = new MessageProducer(manager);
        producer.setTransportManager(manager);
        producer.start();
        Thread.sleep(3000);

        new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 44; i++) {
                    try{
                        send(i);
                        logger.info("producer count is " + producerCount.getAndIncrement());
                    }catch (Exception e){
                        e.printStackTrace();
                        logger.info("error count is " + errProducerCount.getAndIncrement());
                        System.out.println("error i = "+i);
                    }
                }
            }
        }).start();
        CountDownLatch latch = new CountDownLatch(1);
        latch.await();
    }

    /**
     * 测试从指定partition拉取消息，实现指定分组、队列号规则
     *
     * @throws Exception
     */// TODO: 12/21/16 interface removed
    @Test
    public void testPullFromSelectPartition() throws Exception{
        /*ConsumerConfig consumerConfig = new ConsumerConfig();
        messageConsumer = new MessageConsumer(consumerConfig, manager, null);

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        try {
                            messageConsumer.start();
                            //手动拉取消息
                            messageConsumer.pullFromBroker(topic, new MessageListener() {
                                @Override
                                public void onMessage(List<Message> messages) throws Exception {
                                    if (messages != null && !messages.isEmpty()) {
                                        for (Message msg : messages) {
                                            logger.info("message queueId is " + msg.getQueueId());
                                            logger.info("consumer count is " + consumerCount.getAndIncrement());
                                        }
                                    }
                                }
                            }, new ConsumerStrategy() {
                                @Override
                                public String electBrokerGroup(TopicConfig topicConfig, byte dataCenter) {
                                    Random r = new Random();
                                    int size = topicConfig.getGroups().size();
                                    int j = r.nextInt(size);
                                    int i = 0;
                                    for (String group : topicConfig.getGroups()) {
                                        if (j == i) {
                                            return group;
                                        }
                                        i++;
                                    }
                                    return null;
                                }

                                @Override
                                public short electQueueId(String groupName, TopicConfig topicConfig, byte dataCenter) {
                                    short size = topicConfig.getQueues();
                                    short queueId;
                                    Random r = new Random();
                                    queueId = (short)(r.nextInt(size) + 1);
                                    return queueId;
                                }

                                @Override
                                public void pullEnd(String groupName, short queueId) {
                                    if (logger.isDebugEnabled()) {
                                        logger.debug(String.format("pull from %s %d", groupName, queueId));
                                    }
                                }
                            });
                        }catch (Exception e){
                            logger.error("",e);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
        CountDownLatch latch = new CountDownLatch(1);
        latch.await();*/
    }

    /**
     * 非事务的单条发送
     */
    protected void send(int i) throws JMQException {
        Message message = new Message(topic, topic + "_test_wei_" + i, topic + "_rid_" + i);
        producer.send(message);
    }

    /**
     * 实现指定partition接口，选择链路及队列
     */
    public class ProducerLoadBalanceTest implements LoadBalance {
        @Override
        public <T extends ElectTransport> T electTransport(List<T> transports, List<T> errTransports, Message message, byte dataCenter, TopicConfig config) throws JMQException {
            int hashcode = message.getBusinessId().hashCode();
            int size = transports.size();
            int ind;
            if(hashcode==Integer.MIN_VALUE){
                ind = 0;
            }else{
                ind = Math.abs(hashcode) % size;
            }
            logger.info("transport is " + transports.get(ind).getGroup());
            return transports.get(ind);
        }

        @Override
        public <T extends ElectTransport> short electQueueId(T transport, Message message, byte dataCenter, TopicConfig config) throws JMQException {
            logger.info("queueId is " + 1);
            return 1;
        }
    }
}
