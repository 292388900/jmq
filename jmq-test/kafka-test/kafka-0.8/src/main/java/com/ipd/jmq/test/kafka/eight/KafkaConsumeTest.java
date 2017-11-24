package com.ipd.jmq.test.kafka.eight;


import com.ipd.jmq.test.kafka.eight.conf.KafkaProperties;
import com.ipd.jmq.test.kafka.eight.consumer.Consumer;

/**
 * KafkaTest
 *
 * @author luoruiheng
 * @since 2/24/17
 */
public class KafkaConsumeTest {

    public static void main(String[] args) {
        Consumer consumerThread = new Consumer(KafkaProperties.topic);
        consumerThread.start();
    }

}
