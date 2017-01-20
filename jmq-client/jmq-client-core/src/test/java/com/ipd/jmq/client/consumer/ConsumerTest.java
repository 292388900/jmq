package com.ipd.jmq.client.consumer;

import com.ipd.jmq.client.connection.ClusterTransportManager;
import com.ipd.jmq.client.connection.TransportConfig;
import com.ipd.jmq.client.connection.TransportManager;
import com.ipd.jmq.common.message.Message;
import com.ipd.jmq.common.model.ConsumerConfig;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 消费测试.
 *
 * @author lindeqiang
 * @since 14-6-3 下午3:02
 */

public class ConsumerTest {
    private MessageConsumer messageConsumer;

    @Before
    public void setUp() throws Exception {
        String app = "localConsumer";

        TransportConfig config = new TransportConfig();
        config.setApp(app);

        config.setAddress("127.0.0.1:50088");
        config.setUser("jmq");
        config.setPassword("jmq");
        config.setSendTimeout(500);

        config.setEpoll(true);
        TransportManager manager = new ClusterTransportManager(config);
        ConsumerConfig consumerConfig = new ConsumerConfig();
        messageConsumer = new MessageConsumer(consumerConfig, manager, null);
    }

    @Test
    public void testMessageConsumer() throws Exception {
        messageConsumer.start();

        messageConsumer.subscribe("test", new MessageListener() {
            @Override
            public void onMessage(List<Message> messages) throws Exception {
//                if (true) {
//                    throw new RuntimeException("--retry message--");
//                }

                if (messages != null && !messages.isEmpty()) {
                    for (Message msg : messages) {
                        System.out.println("received one message:" + msg.getText());

                    }
                }

                Thread.sleep(1000);
            }
        });

        CountDownLatch latch = new CountDownLatch(1);
        latch.await();
    }

    @Test
    public void removeConsumer() throws Exception {

        for (int i = 0; i < 100000; i++) {

            String app = "localConsumer";
            TransportConfig config = new TransportConfig();
            config.setApp(app);
            config.setAddress("127.0.0.1:50088");
            config.setUser("jmq");
            config.setPassword("jmq");
            config.setSendTimeout(500);
            config.setEpoll(true);

            TransportManager manager = new ClusterTransportManager(config);
            ConsumerConfig consumerConfig = new ConsumerConfig();
            messageConsumer = new MessageConsumer(consumerConfig, manager, null);
            messageConsumer.start();

            //final CountDownLatch latch = new CountDownLatch(1);

            messageConsumer.subscribe("test", new MessageListener() {
                @Override
                public void onMessage(List<Message> messages) throws Exception {

                    if (messages != null && !messages.isEmpty()) {
                        for (Message msg : messages) {

                        }

                    }

                    Thread.sleep(1000);
                   // latch.countDown();
                    System.out.println("--count down--");
                }
            });

            Thread.sleep(2000);

           // latch.await();

            messageConsumer.stop();

            System.out.println("-- consume over--");
        }
    }


}
