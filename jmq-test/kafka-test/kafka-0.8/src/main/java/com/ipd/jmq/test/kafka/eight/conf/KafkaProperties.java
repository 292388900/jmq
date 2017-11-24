package com.ipd.jmq.test.kafka.eight.conf;

/**
 * KafkaProperties
 *
 * @author luoruiheng
 * @since 2/24/17
 */
public interface KafkaProperties {

    final static String zkConnect = "192.168.159.116:2181/jmq";
    final static String groupId = "hengCC";
    final static String topic = "hello";
    final static String kafkaServerURL = "10.42.0.1";
    final static int kafkaServerPort = 50088;
    final static int kafkaProducerBufferSize = 64*1024;
    final static int connectionTimeOut = 100000;
    final static int reconnectInterval = 10000;
    final static String clientId = "hengP";

}
