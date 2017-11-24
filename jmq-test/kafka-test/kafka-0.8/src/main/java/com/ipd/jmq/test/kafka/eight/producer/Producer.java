
package com.ipd.jmq.test.kafka.eight.producer;


import com.ipd.jmq.test.kafka.eight.conf.KafkaProperties;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class Producer extends Thread {
    private final kafka.javaapi.producer.Producer<Integer, String> producer;
    private final String topic;
    private final Properties props = new Properties();

    public Producer(String topic) {
        props.put("client.id", KafkaProperties.clientId);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list", KafkaProperties.kafkaServerURL + ":" + KafkaProperties.kafkaServerPort);
        // Use random partitioner. Don't need the key type. Just set it to Integer.
        // The message is of type String.
        producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
        this.topic = topic;
    }

    public void run() {
        int messageNo = 1;
        while (messageNo <= 10) {
            String messageStr = "Message_" + messageNo + "_" + System.nanoTime();
            producer.send(new KeyedMessage<Integer, String>(topic, messageStr));
            messageNo++;
            System.out.println(messageStr + " sent successfully.");
        }
    }

}
