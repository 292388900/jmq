package com.ipd.jmq.kafka.demo;

import com.ipd.jmq.kafka.demo.conf.KafkaProperties;
import com.ipd.jmq.kafka.demo.consumer.Consumer;

/**
 * KafkaTest
 *
 * @author luoruiheng
 * @since 2/24/17
 */
public class KafkaConsumeTest {

    public static void main(String[] args) {
        Consumer consumer = new Consumer(KafkaProperties.topic);

        while (!Thread.interrupted()) {
            consumer.doWork();
        }

    }

}
