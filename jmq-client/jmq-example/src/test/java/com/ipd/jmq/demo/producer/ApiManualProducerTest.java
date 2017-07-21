package com.ipd.jmq.demo.producer;

import com.ipd.jmq.client.connection.ClusterTransportManager;
import com.ipd.jmq.client.connection.TransportConfig;
import com.ipd.jmq.client.connection.TransportManager;
import com.ipd.jmq.client.producer.AsyncSendCallback;
import com.ipd.jmq.client.producer.LoadBalance;
import com.ipd.jmq.client.producer.MessageProducer;
import com.ipd.jmq.common.cluster.*;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.Message;
import com.ipd.jmq.common.network.v3.command.PutMessage;
import com.ipd.jmq.common.network.v3.netty.failover.ElectTransport;
import com.ipd.jmq.common.network.v3.command.Command;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * 生产测试
 */
public class ApiManualProducerTest {
    private static final Logger logger = Logger.getLogger(ApiProducerTest.class);
    private TransportManager manager;
    private MessageProducer producer;
    // @Value("${jmq.producer.topic}")
    private String topic = "zkptest";
    // @Value("${jmq.producer.app}")
    private String app = "jmq";
    //  @Value("${jmq.address}")
    private String address = "10.12.167.40:50088";

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
//        producer.setLoadBalance(new TestProducerLoadBalance());
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
        for (int i = 0; ; i++) {
            try {
                //sendSequential(i);
                send(i);
                logger.info("成功发送一条消息！！");
                Thread.sleep(500);
            } catch (Exception e) {
                e.printStackTrace();
                try {
                    Thread.sleep(2000);
                } catch (Exception ignored) {

                }
            }
        }
    }

    @Test
    public void testAsyncSend() throws JMQException {
        for (int i = 0; ; i++) {
            try {
                Message message = new Message(topic, "消息内容" + i, "业务ID" + i);
                producer.sendAsync(message, new AsyncSendCallback() {
                    @Override
                    public void success(PutMessage putMessage, Command response) {
                        logger.info("成功发送一条消息！！" + putMessage.getMessages()[0].getBusinessId());
                    }

                    @Override
                    public void exception(PutMessage putMessage, Throwable cause) {
                        logger.info("err一条消息！！" + putMessage.getMessages()[0].getBusinessId());
                    }
                });
                logger.info("发送一条消息！！" + i);
                Thread.sleep(500);
            } catch (Exception e) {
                e.printStackTrace();
                try {
                    Thread.sleep(2000);
                } catch (Exception ignored) {

                }
            }
        }
    }

    /**
     * 非事务的单条发送
     */
    protected void send(int i) throws JMQException {
        Message message = new Message(topic, "消息内容" + i, "业务ID" + i);
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

    public class TestProducerLoadBalance implements LoadBalance {
        @Override
        public <T extends ElectTransport> T electTransport(List<T> transports, List<T> errTransports, Message message, byte dataCenter, TopicConfig config) throws JMQException {
            int hashcode = message.getBusinessId().hashCode();
            int size = transports.size();
            int ind = 0;
            if (hashcode == Integer.MIN_VALUE) {
                ind = 0;
            } else {
                ind = Math.abs(hashcode) % size;
            }
            return transports.get(ind);
        }

        @Override
        public <T extends ElectTransport> short electQueueId(T transport, Message message, byte dataCenter, TopicConfig config) throws JMQException {
//            int hashcode = message.getBusinessId().hashCode();
//            short size = config.getQueues();
//            short queueId = 1;
//            if(hashcode==Integer.MIN_VALUE){
//                queueId = 1;
//            }else{
//                queueId = (short)(Math.abs(hashcode) % size + 1);
//            }
//            return queueId;
            return 1;
        }
    }

    @Test
    public void sort() {
        List<BrokerGroup> groups = new ArrayList<BrokerGroup>(5);
        BrokerGroup group1 = new BrokerGroup();
        group1.setPermission(Permission.FULL);
        group1.setGroup("group1");
        Broker broker1 = new Broker();
        broker1.setIp("ip1");
        broker1.setPort(50088);
        broker1.setRole(ClusterRole.MASTER);
        group1.addBroker(broker1);

        BrokerGroup group2 = new BrokerGroup();
        group2.setPermission(Permission.READ);
        group2.setGroup("group2");
        Broker broker2 = new Broker();
        broker2.setIp("ip2");
        broker2.setPort(50088);
        broker2.setRole(ClusterRole.MASTER);
        group2.addBroker(broker2);


        BrokerGroup group3 = new BrokerGroup();
        group3.setPermission(Permission.WRITE);
        group3.setGroup("group3");
        Broker broker3 = new Broker();
        broker3.setIp("ip3");
        broker3.setPort(50088);
        broker3.setRole(ClusterRole.MASTER);
        group3.addBroker(broker3);


        BrokerGroup group4 = new BrokerGroup();
        group4.setPermission(Permission.READ);
        group4.setGroup("group4");
        Broker broker4 = new Broker();
        broker4.setIp("ip4");
        broker4.setPort(50088);
        broker4.setRole(ClusterRole.MASTER);
        group4.addBroker(broker4);

        groups.add(group1);
        groups.add(group2);
        groups.add(group3);
        groups.add(group4);

        if (groups != null && groups.size() > 0) {

            String[] addresses = new String[groups.size()];
            for (int i = 0; i < groups.size(); i++) {
                for (int j = groups.size() - 1; j > i; j--) {
                    BrokerGroup low = groups.get(j);
                    BrokerGroup high = groups.get(j - 1);
                    if (low.getPermission().ordinal() > high.getPermission().ordinal()) {
                        groups.set(j, high);
                        groups.set(j - 1, low);
                    }
                }
                BrokerGroup group = groups.get(i);
                addresses[i] = group.getMaster().getIp() + ":" + group.getMaster().getPort();
                System.out.println(addresses[i] + ":" + group.getPermission());
            }
        }
    }
}

