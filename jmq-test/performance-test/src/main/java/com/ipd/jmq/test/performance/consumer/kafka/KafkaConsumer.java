package com.ipd.jmq.test.performance.consumer.kafka;

import com.ipd.jmq.test.performance.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by zhangkepeng on 16-12-9.
 */
public class KafkaConsumer implements Consumer {
    private final static Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    private org.apache.kafka.clients.consumer.KafkaConsumer kafkaConsumer;
    private String topic;
    private static AtomicLong count = new AtomicLong();


    @Override
    public long getCountValue() {
        return count.get();
    }

    @Override
    public void init() {
        Properties props = new Properties();
        InputStream in = null;
        try {
            URL url = KafkaConsumer.class.getClassLoader().getResource("kafka-consumer.properties");
            if (url == null) {
                url = KafkaConsumer.class.getClassLoader().getResource("conf/kafka-consumer.properties");
            }
            in = url.openStream();
            props.load(in);
            kafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer(props);
            kafkaConsumer.subscribe(Collections.singletonList(topic));
        } catch (Exception e) {
            logger.error("",e);
        }finally {
            if(in != null){
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void shutdown() {
        if (kafkaConsumer != null) kafkaConsumer.close();
    }

    @Override
    public void run() {
        ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
        for (ConsumerRecord<String, String> record : records) {
            if (logger.isDebugEnabled()) {
                String thread = Thread.currentThread().getName() + " OWNS Partition: " + record.partition() + " ---- ";
                logger.debug(thread + String.format("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value()));
            }
        }
        count.getAndAdd(records.count());
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    /**
     * 负载低的情况下可以每个线程消费多个partition。
     * 但负载高的情况下，Consumer 线程数最好和Partition数量保持一致。
     * 如果还是消费不过来，应该再开 Consumer 进程，进程内线程数同样和分区数一致。
     */
}
