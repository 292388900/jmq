package com.ipd.jmq.test.performance.consumer.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * MultiThreadKafkaConsumer
 *
 * @author luoruiheng
 * @since 3/1/17
 */
public class MultiThreadKafkaConsumer {
    private final static Logger logger = LoggerFactory.getLogger(MultiThreadKafkaConsumer.class);

    private org.apache.kafka.clients.consumer.KafkaConsumer kafkaConsumer;
    private ExecutorService executors = null;
    private String topic = "hello";
    private int threadNum = 5;



    private void init() {
        Properties props = new Properties();
        InputStream in = null;
        try {
            URL url = MultiThreadKafkaConsumer.class.getClassLoader().getResource("kafka-consumer.properties");
            if (url == null) {
                url = MultiThreadKafkaConsumer.class.getClassLoader().getResource("conf/kafka-consumer.properties");
            }
            in = url.openStream();
            props.load(in);
            kafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer(props);
            kafkaConsumer.subscribe(Collections.singletonList(topic));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void shutdown() {
        if (kafkaConsumer != null) {
            kafkaConsumer.close();
        }
        if (executors != null) {
            executors.shutdownNow();
        }
    }

    private void doWork() {
        executors = Executors.newFixedThreadPool(threadNum);

        while (!Thread.interrupted()) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for (final ConsumerRecord<String, String> record : records) {
                executors.submit(new OnReceiveTask(record));
            }
        }
    }


    private class OnReceiveTask implements Runnable {

        private ConsumerRecord<String, String> record;

        public OnReceiveTask(ConsumerRecord<String, String> record) {
            this.record = record;
        }

        @Override
        public void run() {
            String thread = Thread.currentThread().getName() + " OWNS Partition: " + record.partition() + " ---- ";
            System.out.println(thread + String.format("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value()));
        }

    }


    public static void main(String[] args) {
        MultiThreadKafkaConsumer consumer = new MultiThreadKafkaConsumer();
        consumer.init();
        consumer.doWork();

        try {
            Thread.sleep(10000);
        } catch (InterruptedException ignored) {

        }
        consumer.shutdown();
    }

}
