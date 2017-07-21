package com.ipd.jmq.kafka.demo;

import com.ipd.jmq.kafka.demo.conf.KafkaProperties;
import com.ipd.jmq.kafka.demo.producer.Producer;

/**
 * KafkaTest
 *
 * @author luoruiheng
 * @since 2/24/17
 */
public class KafkaProduceTest {


    public static void main(String[] args) {
        boolean isAsync = false;
        Producer producer = new Producer(KafkaProperties.topic, isAsync);
        producer.start();

        while (true) {
            try {
                Thread.sleep(100);
            } catch (Exception ignored) {

            }
        }

    }

}
