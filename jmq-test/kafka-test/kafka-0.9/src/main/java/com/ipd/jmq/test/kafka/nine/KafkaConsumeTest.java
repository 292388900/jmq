package com.ipd.jmq.test.kafka.nine;


import com.ipd.jmq.test.kafka.nine.conf.KafkaProperties;
import com.ipd.jmq.test.kafka.nine.consumer.Consumer;

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
