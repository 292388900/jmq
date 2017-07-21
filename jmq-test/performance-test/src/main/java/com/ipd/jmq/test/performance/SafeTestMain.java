package com.ipd.jmq.test.performance;

import com.ipd.jmq.test.performance.safe.SafeConsumer;
import com.ipd.jmq.test.performance.safe.SafeProducer;
import com.ipd.jmq.test.performance.safe.check.ProduceDataCheck;
import com.ipd.jmq.toolkit.lang.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

/**
 * Created by lining11 on 2017/5/9.
 */
public class SafeTestMain {
    private static Logger log = LoggerFactory.getLogger(SafeTestMain.class);

    InputStream in = null;

    public static void main(String[] args) {

        SafeTestMain main = new SafeTestMain();
        main.start();
    }

    public void start() {
        Properties p = new Properties();
        URL url = ProducerMain.class.getClassLoader().getResource("safe.properties");
        if (url == null) {
            url = ProducerMain.class.getClassLoader().getResource("conf/safe.properties");
        }
        try {
            in = url.openStream();
        } catch (IOException e) {
            log.error("get resource error!", e);
        }
        try {
            p.load(in);
        } catch (IOException e) {
            log.error("load config error!", e);
        }

        String lineSizeStr = p.getProperty("line.size");
        int lineSize = Integer.parseInt(lineSizeStr);

        String produce = p.getProperty("producer.safe.produce");
        if (!Strings.isNullOrEmpty(produce) && ("true".equalsIgnoreCase(produce) || "true" == produce)) {

            String topic = p.getProperty("producer.safe.topic");
            String app = p.getProperty("producer.safe.app");
            String address = p.getProperty("producer.safe.address");
            String user = p.getProperty("producer.safe.user");
            String password = p.getProperty("producer.safe.password");
            String ack = p.getProperty("producer.safe.ack");

            SafeProducer safeProducer = new SafeProducer();
            safeProducer.setTopic(topic);
            safeProducer.setApp(app);
            safeProducer.setAddress(address);
            safeProducer.setUser(user);
            safeProducer.setPassword(password);
            safeProducer.setFileSize(lineSize * lineSize);
            safeProducer.setAck(Integer.parseInt(ack));
            try {
                safeProducer.init();
                safeProducer.testSend();
            } catch (Exception e) {
                log.error("init error!", e);
            }

        }

        String consume = p.getProperty("consumer.safe.consume");
        if (!Strings.isNullOrEmpty(consume) && ("true".equalsIgnoreCase(consume) || "true" == consume)) {

            String topic = p.getProperty("consumer.safe.topic");
            String app = p.getProperty("consumer.safe.app");
            String address = p.getProperty("consumer.safe.address");
            String user = p.getProperty("consumer.safe.user");
            String password = p.getProperty("consumer.safe.password");

            SafeConsumer safeConsumer = new SafeConsumer();
            safeConsumer.setTopic(topic);
            safeConsumer.setApp(app);
            safeConsumer.setAddress(address);
            safeConsumer.setUser(user);
            safeConsumer.setPassword(password);
            safeConsumer.setFileSize(lineSize * lineSize);
            try {
                safeConsumer.init();
                safeConsumer.subscribe(topic);
            } catch (Exception e) {
                log.error("init error!", e);
            }

        }

        String produceCheck = p.getProperty("producer.safe.produceCheck");
        if (!Strings.isNullOrEmpty(produceCheck) && ("true".equalsIgnoreCase(produceCheck) || "true" == produceCheck)) {

            ProduceDataCheck produceDataCheck = new ProduceDataCheck();
            produceDataCheck.setLineSize(lineSize);
            produceDataCheck.setSourceFolder("produce");
            try {
                produceDataCheck.start();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

        String consumeCheck = p.getProperty("consumer.safe.consumeCheck");
        if (!Strings.isNullOrEmpty(consumeCheck) && ("true".equalsIgnoreCase(consumeCheck) || "true" == consumeCheck)) {
            ProduceDataCheck produceDataCheck = new ProduceDataCheck();
            produceDataCheck.setLineSize(lineSize);
            produceDataCheck.setSourceFolder("consume");
            try {
                produceDataCheck.start();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }
}
