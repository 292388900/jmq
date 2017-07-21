package com.ipd.jmq.kafka.demo.conf;

/**
 * KafkaProperties
 *
 * @author luoruiheng
 * @since 2/24/17
 */
public interface KafkaProperties {

    final static String zkConnect = "127.0.0.1:2181";
    final static String groupId = "hengC";
    final static String topic = "heng";
    final static String kafkaServerURL = "10.42.0.1";
    final static int kafkaServerPort = 50088;
    final static int kafkaProducerBufferSize = 64*1024;
    final static int connectionTimeOut = 100000;
    final static int reconnectInterval = 10000;
    final static String clientId = "hengP";

}
