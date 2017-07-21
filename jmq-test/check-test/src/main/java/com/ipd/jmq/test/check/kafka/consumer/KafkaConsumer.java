package com.ipd.jmq.test.check.kafka.consumer;

import com.ipd.jmq.test.check.ClientConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhangkepeng on 2017/1/11.
 */
public class KafkaConsumer implements ClientConsumer {
    private final static Logger logger = LoggerFactory.getLogger("consumer");
    private org.apache.kafka.clients.consumer.KafkaConsumer<String, String> kafkaConsumer;
    private String topic;
    private int threadNums;
    private ExecutorService executor;
    private CountDownLatch latch = new CountDownLatch(1);

    @Override
    public void open() throws Exception {
        init();
    }

    private void init() {
        Properties p = null;
        InputStream configIn = null;
        InputStream in = null;
        try {
            URL configUrl = KafkaConsumer.class.getClassLoader().getResource("check.properties");
            if (configUrl == null) {
                configUrl = KafkaConsumer.class.getClassLoader().getResource("conf/check.properties");
            }
            configIn = configUrl.openStream();
            Properties configPro = new Properties();
            configPro.load(configIn);
            topic = configPro.getProperty("consumer.topic", "test");
            threadNums = Integer.valueOf(configPro.getProperty("consumer.high.thread.num", "0"));

            URL url = KafkaConsumer.class.getClassLoader().getResource("kafka-consumer.properties");
            if (url == null) {
                url = KafkaConsumer.class.getClassLoader().getResource("conf/kafka-consumer.properties");
            }
            in = url.openStream();
            p = new Properties();
            p.load(in);
            kafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(p);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }
            if (configIn != null) {
                try {
                    configIn.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

    @Override
    public void consume() throws Exception {
        if (topic == null) {
            throw new Exception("topic is null, no init");
        }
        // now launch all the threads
        //
        executor = Executors.newFixedThreadPool(threadNums);
        // now create an object to consume the messages
        //
        for (int i = 0; i < threadNums; i++) {
            executor.submit(new ConsumerStream(kafkaConsumer));
        }
        try {
            latch.await();
        } catch (InterruptedException ie) {
            logger.error("interrupt exception", ie);
        }
    }

    @Override
    public void close() throws Exception {
        latch.countDown();
        if (kafkaConsumer != null) kafkaConsumer.close();
        if (executor != null) executor.shutdown();
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown, exiting uncleanly");
        }
    }

    public class ConsumerStream implements Runnable {
        private org.apache.kafka.clients.consumer.KafkaConsumer<String, String> kafkaConsumer;

        public ConsumerStream(org.apache.kafka.clients.consumer.KafkaConsumer<String, String> kafkaConsumer) {
            this.kafkaConsumer = kafkaConsumer;
        }

        public void run() {
            kafkaConsumer.subscribe(Collections.singletonList(topic));
            while (!Thread.interrupted()){
                ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    dealBody(record.value());
//                    logger.info(String.format("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value()));
                }
            }
        }

        public void dealBody(String body) {
            String messageID = null;
            // Thread.sleep(500);
            String[] ar = body.split("&")[0].split("=");
            String counter = null;
            if (ar.length != 2) {
                logger.info(body + " " + messageID);
            } else {
                counter = ar[1];
                long c = Long.parseLong(counter);
                if (c == 1000 || c == 2001) {
                    //埋点用于检查程序的正确性
                    logger.info("received message" + c + "");
                } else {
                    logger.info("received message@" + c + "#");
                }
            }

        }
    }
}
