package com.ipd.jmq.test.performance;

import com.ipd.jmq.test.performance.producer.ProducerStat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

/**
 * Created by zhangkepeng on 17-1-6.
 */
public class ProducerMain {
    private static Logger log = LoggerFactory.getLogger(ProducerMain.class);

    public static String PRODUCER_KAFKA = "kafka";
    public static String PRODUCER_JMQ = "jmq";

    public void produce(){
        Properties p = new Properties();
        InputStream in = null;
        int threadSize = 1;
        int msgNum= 0;
        int msgSize = 1024*1024*2-200;
        long maxUsedLimited = 100;
        long timeLimited = 1000;

        String producerType = PRODUCER_JMQ;
        String topicStr = "";
        String appStr = "";
        long sliceInterval = 5000;
        boolean isAsync = false;
        try {

            URL url = ProducerMain.class.getClassLoader().getResource("performance.properties");
            if(url == null){
                url = ProducerMain.class.getClassLoader().getResource("conf/performance.properties");
            }
            in = url.openStream();
            p.load(in);

            msgNum = Integer.valueOf(p.getProperty("producer.send.max.msg.count", "0"));
            threadSize = Integer.valueOf(p.getProperty("producer.thread.num", "0"));
            sliceInterval = Long.valueOf(p.getProperty("producer.slice.interval", "5000"));
            isAsync = Boolean.valueOf(p.getProperty("producer.async", "false"));

            topicStr = p.getProperty("producer.topic");
            appStr = p.getProperty("producer.app");
            msgSize = Integer.valueOf(p.getProperty("producer.send.body.len", "1024"));
            producerType = p.getProperty("producer.type", "kafka");
            maxUsedLimited = Long.valueOf(p.getProperty("producer.max.limited", "1000"));
            timeLimited = Long.valueOf(p.getProperty("producer.time.limited", "1000"));
        } catch (Exception e) {
            log.error("",e);
        }finally {
            if(in != null){
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        final String topic = topicStr;
        final String app = appStr;
        ProducerStat stat = new ProducerStat(producerType, sliceInterval);

        log.info(String.format("topic:%s, msgNum:%d, threadSize:%d, app:%s, bodySize:%d", topic, msgNum, threadSize, app, msgSize));
        stat.stat(producerType, threadSize, topic, msgSize, msgNum, isAsync, maxUsedLimited, timeLimited);

    }


    public static void main(String[] args) {

        ProducerMain pm = new ProducerMain();
        pm.produce();
    }
}
