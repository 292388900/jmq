package com.ipd.jmq.test.performance;

import com.ipd.jmq.test.performance.consumer.ConsumerStat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

/**
 * Created by dingjun on 15-11-20.
 */
public class ConsumerMain {
    private static Logger log = LoggerFactory.getLogger(ConsumerMain.class);

    public static String CONSUMER_HIGH_KAFKA = "high_kafka";
    public static String CONSUMER_SIMPLE_KAFKA = "simple_kafka";
    public static String CONSUMER_JMQ = "jmq";

    public void consume() {
        Properties p = new Properties();
        InputStream in = null;
        int threadSize = 1;
        int msgNum = 0;
        int msgSize = 1024 * 1024 * 2 - 200;

        String consumerType = CONSUMER_JMQ;
        String topicStr = "";
        String appStr = "";
        long sliceInterval = 5000;
        try {

            URL url = ProducerMain.class.getClassLoader().getResource("performance.properties");
            if (url == null) {
                url = ProducerMain.class.getClassLoader().getResource("conf/performance.properties");
            }
            in = url.openStream();
            p.load(in);

            threadSize = Integer.valueOf(p.getProperty("consumer.thread.num", "0"));
            sliceInterval = Long.valueOf(p.getProperty("consumer.slice.interval", "5000"));

            topicStr = p.getProperty("consumer.topic");
            appStr = p.getProperty("consumer.app");
            consumerType = p.getProperty("consumer.type", CONSUMER_JMQ);
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
        final String topic = topicStr;
        final String app = appStr;
        ConsumerStat stat = new ConsumerStat(consumerType, sliceInterval);

        log.info(String.format("topic:%s, msgNum:%d, threadSize:%d, app:%s, bodySize:%d", topic, msgNum, threadSize, app, msgSize));
        stat.stat(consumerType, threadSize, topic, app);

    }


    public static void main(String[] args) {

        ConsumerMain cm = new ConsumerMain();
        cm.consume();
    }
}
