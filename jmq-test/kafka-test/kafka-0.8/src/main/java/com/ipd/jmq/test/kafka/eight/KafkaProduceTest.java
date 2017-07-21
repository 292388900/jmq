package com.ipd.jmq.test.kafka.eight;


import com.ipd.jmq.test.kafka.eight.conf.KafkaProperties;
import com.ipd.jmq.test.kafka.eight.producer.Producer;

/**
 * KafkaTest
 *
 * @author luoruiheng
 * @since 2/24/17
 */
public class KafkaProduceTest {


    public static void main(String[] args) {
        Producer producerThread = new Producer(KafkaProperties.topic);
        producerThread.start();

        while (true) {
            try {
                Thread.sleep(100);
            } catch (Exception ignored) {

            }
        }

    }

}
