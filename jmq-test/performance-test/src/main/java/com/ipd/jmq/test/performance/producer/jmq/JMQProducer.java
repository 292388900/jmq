package com.ipd.jmq.test.performance.producer.jmq;

import com.ipd.jmq.client.connection.ClusterTransportManager;
import com.ipd.jmq.client.connection.TransportConfig;
import com.ipd.jmq.client.connection.TransportManager;
import com.ipd.jmq.client.producer.MessageProducer;
import com.ipd.jmq.common.message.Message;
import com.ipd.jmq.test.performance.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

/**
 * Created by dingjun on 15-7-24.
 */
public class JMQProducer implements Producer {
    private static Logger log = LoggerFactory.getLogger(JMQProducer.class);
    MessageProducer producer = null;
    TransportManager manager = null;
    protected boolean compressed=false;
    String app;

    @Override
    public void init() {
        /**
         * mq.user=usr
         mq.password=0C0C6CA8
         mq.address=192.168.209.78:50088

         mq.consumer.app=mqtest
         mq.consumer.topic=mqtest_performance

         mq.producer.app=mqtest
         mq.producer.topic=mqtest_performance
         */
        InputStream in = null;
        Properties p = new Properties();
        try {

            URL url = JMQProducer.class.getClassLoader().getResource("jmq-producer.properties");
            if (url == null) {
                url = JMQProducer.class.getClassLoader().getResource("conf/jmq-producer.properties");
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

        int sendTimeout = 50000;
        String compressedStr = p.getProperty("mq.send.compressed");
        if (compressedStr != null && !compressedStr.isEmpty()) {
            try {
                compressed = Boolean.valueOf(compressedStr);
            } catch (Exception ignored) {
            }
        }
        TransportConfig config = new TransportConfig();
        config.setApp(p.getProperty("mq.producer.app"));
        this.app = config.getApp();
        config.setAddress(p.getProperty("mq.address"));
        config.setUser(p.getProperty("mq.user"));
        config.setPassword(p.getProperty("mq.password"));
        config.setSendTimeout(sendTimeout);
        config.setWorkerThreads(Integer.parseInt(p.getProperty("netty.worker.thread")));
        config.setEpoll(Boolean.parseBoolean(p.getProperty("netty.epoll")));

        manager = new ClusterTransportManager(config);
        try {
            manager.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer = new MessageProducer();
        producer.setTransportManager(manager);
        try {
            producer.start();
        } catch (Exception e) {
            log.error("", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void send(String topic, String text) {
        Message msg = new Message();
        msg.setTopic(topic);
        msg.setApp(app);
        if (!compressed){
            msg.body(text.getBytes());
        }  else {
            msg.setText(text);
        }
        try {
            producer.send(msg);
        } catch (Exception e) {
            log.error("send exception");
        }
    }

    public void close(){
        manager.stop();
    }

}
