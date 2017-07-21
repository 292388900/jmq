package com.ipd.jmq.test.performance.consumer.jmq;

import com.ipd.jmq.client.connection.ClusterTransportManager;
import com.ipd.jmq.client.connection.TransportConfig;
import com.ipd.jmq.common.model.ConsumerConfig;
import com.ipd.jmq.client.consumer.MessageConsumer;
import com.ipd.jmq.client.consumer.MessageListener;
import com.ipd.jmq.common.message.Message;
import com.ipd.jmq.test.performance.consumer.Consumer;
import com.ipd.jmq.test.performance.producer.jmq.JMQProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by lining11 on 2015/11/4.
 */
public class JMQConsumer implements Consumer {

    static AtomicLong count = new AtomicLong();

    private static Logger log = LoggerFactory.getLogger(JMQConsumer.class);

    ClusterTransportManager clusterTransportManager;

    private MessageConsumer messageConsumer;

    private Properties properties;

    @Override
    public long getCountValue() {
        return count.get();
    }

    @Override
    public void init() {
        InputStream in = null;
        Properties p = new Properties();
        try {

            URL url = JMQProducer.class.getClassLoader().getResource("jmq-consumer.properties");
            if (url == null) {
                url = JMQProducer.class.getClassLoader().getResource("conf/jmq-consumer.properties");
            }
            in = url.openStream();
            p.load(in);


        } catch (Exception e) {
            log.error("", e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        properties = p;

    }

    public void start() {

        TransportConfig config = new TransportConfig();
        config.setAddress(properties.getProperty("jmq.address"));
        config.setUser(properties.getProperty("jmq.user"));
        config.setPassword(properties.getProperty("jmq.password"));

        config.setApp(properties.getProperty("jmq.app"));


        clusterTransportManager = new ClusterTransportManager(config);

        try {
            clusterTransportManager.start();
        } catch (Exception e) {
            log.error("", e);
        }

        ConsumerConfig consumerConfig = new ConsumerConfig();
        messageConsumer = new MessageConsumer(consumerConfig, clusterTransportManager, null);

        try {
            messageConsumer.start();
        } catch (Exception e) {
            log.error("", e);
        }
    }

    public void subscribe(String topic,MessageListener messageListener){

        messageConsumer.subscribe(topic,messageListener);
    }

    @Override
    public void run(){
        //init();

        start();

        String topic = properties.getProperty("jmq.topic");

        DefaltMessageListener listener = new DefaltMessageListener(topic);

        subscribe(topic,listener);



    }

    class DefaltMessageListener implements MessageListener {

        String topic = null;

        public DefaltMessageListener(String topic){
            this.topic = topic;
        }

        public void onMessage(List<Message> messages) throws Exception {
            for (Message message:messages) {
                if (message.getTopic().equalsIgnoreCase(this.topic)){
                    count.incrementAndGet();
                }
            }
            //throw new RuntimeException("");
        }
    }



}
