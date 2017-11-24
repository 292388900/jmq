package com.ipd.jmq.client;

import com.ipd.jmq.client.connection.ClusterTransportManager;
import com.ipd.jmq.client.connection.TransportConfig;
import com.ipd.jmq.client.connection.TransportManager;
import com.ipd.jmq.common.model.ConsumerConfig;
import com.ipd.jmq.client.consumer.MessageConsumer;
import com.ipd.jmq.client.consumer.MessageListener;
import com.ipd.jmq.client.producer.MessageProducer;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.Message;
import com.ipd.jmq.toolkit.retry.RetryPolicy;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created with IntelliJ IDEA.
 * User: weiqisong
 * Date: 14-6-23
 * Time: 下午3:34
 * To change this template use File | Settings | File Templates.
 */
public class ProcducerAndConsumerTest {

    private MessageProducer producer;
    private String topic;
    private String app;
    private int sendTimeout;
    private MessageConsumer messageConsumer;

    @Before
    public void setUp() throws Exception {
        app = "prowei1";
        topic = "testwei";
        sendTimeout = 50000;

        TransportConfig config = new TransportConfig();
        config.setApp(app);

        //TODO 设置broker地址
//        config.setAddress("192.168.209.78:50088");
        config.setAddress("10.12.113.39:50088");
        config.setUser("usr");
        config.setPassword("0C0C6CA8");
        config.setSendTimeout(sendTimeout);

        //TODO Linux环境下设置为true
        config.setEpoll(false);

        TransportManager manager = new ClusterTransportManager(config);

        producer = new MessageProducer();
        producer.setTransportManager(manager);
        producer.start();
        Thread.sleep(2000*3);



        String app = "imwei1";

        TransportConfig consumerConfig = new TransportConfig();
        consumerConfig.setApp(app);
        //TODO 设置broker地址
        consumerConfig.setAddress("10.12.113.39:50088");
//        config.setAddress("192.168.209.78:50088");
        consumerConfig.setUser("usr");
        consumerConfig.setPassword("0C0C6CA8");
        consumerConfig.setSendTimeout(500);

        //TODO linux环境下设置为true
        consumerConfig.setEpoll(false);


        TransportManager consumerManager = new ClusterTransportManager(consumerConfig);
        ConsumerConfig consumerConfig2 = new ConsumerConfig();
        consumerConfig2.setRetryPolicy(new RetryPolicy(1,1,1,true,2.0,1));
        messageConsumer = new MessageConsumer(consumerConfig2, consumerManager, null);
    }

    @Test
    public void testName() throws Exception {

        new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 100; i++) {
                    try{
                        send(i);
                        Thread.sleep(2000L);
//                this.sendTransaction(i);
//                this.batchSend(10,10);
                    }catch (Exception e){
                        e.printStackTrace();
                        System.out.println("error i = "+i);
                    }
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    messageConsumer.start();
                } catch (Exception e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }

                messageConsumer.subscribe("testwei", new MessageListener() {
                    @Override
                    public void onMessage(List<Message> messages) throws Exception {

                        if (messages != null && !messages.isEmpty()) {
                            for (Message msg : messages) {
                                /*if(msg.getQueueId()==255){
                                    System.out.println("e3");
                                    throw new Exception();
                                } else {
                                    System.out.println("received one message:"+msg.getQueueId()+"-----" + msg.getText());
                                }*/
                            }
                        }

                    }
                });

            }
        }).start(); ;
        CountDownLatch latch = new CountDownLatch(1);
        latch.await();
    }
    protected void send(int i) throws JMQException {
        Message message = new Message(topic, topic + "_test_wei_" + i, topic + "_rid_" + i);
        if(i%2==1)message.setPriority((Short.valueOf((short)5).byteValue()));
        producer.send(message);
    }
}
