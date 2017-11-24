package com.ipd.jmq.test.kafka.nine.conf;

/**
 * KafkaProperties
 *
 * @author luoruiheng
 * @since 2/24/17
 */
public interface KafkaProperties {

    final static String groupId = "app4Consumer_kafka";
    final static String topic = "hello";
    final static String kafkaServerURL = "192.168.1.5";
    final static int kafkaServerPort = 50088;
    final static String clientId = "app4Product_kafka";

}
