package com.ipd.jmq.demo.consumer;

import com.ipd.jmq.client.connection.ClusterTransportManager;
import com.ipd.jmq.client.connection.TransportConfig;
import com.ipd.jmq.client.connection.TransportManager;
import com.ipd.jmq.common.model.ConsumerConfig;
import com.ipd.jmq.client.consumer.ConsumerStrategy;
import com.ipd.jmq.client.consumer.MessageConsumer;
import com.ipd.jmq.client.consumer.MessageListener;
import com.ipd.jmq.common.cluster.TopicConfig;
import com.ipd.jmq.common.message.Message;
import com.ipd.jmq.common.model.Executor;
import com.ipd.jmq.demo.DefaultMessageListener;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * Created by dingjun on 15-10-28.
 */
public class ApiManualConsumerTest {
    private static final Logger logger = Logger.getLogger(ApiConsumerTest.class);
    //连接管理器
    private TransportManager manager;
    //消息消费者，同一个App可以使用同一个消费者订阅多个主题
    private MessageConsumer messageConsumer;
    //消息处理器
    private MessageListener messageListener = new DefaultMessageListener();

    //消费者app。 JMQ中的app等同于MQ中的systemId
    private String app = "jmq";
    //消费者topic。JMQ中的topic等同于MQ中的destination
    private String topic = "jmq_test";
    private String address = "10.12.166.51:50088";

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

    // TODO: 12/21/16 interface removed
    @Test
    public void testPullFromBroker() throws Exception{
        /*messageConsumer.start();
        for (;;){
            try {
                //手动拉取消息
                messageConsumer.pullFromBroker(topic, new MessageListener() {
                    @Override
                    public void onMessage(List<Message> messages) throws Exception {
                        logger.info("pull size:" + messages.size() + ";" + messages.get(0).getQueueId());
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
                        short queueId = 1;
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
        }*/
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
