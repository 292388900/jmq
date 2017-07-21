package com.ipd.jmq.test.check.kafka.producer;

import com.ipd.jmq.test.check.ClientProducer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by zhangkepeng on 2017/1/11.
 */
public class KafkaProducer implements ClientProducer {
    private static final Logger logger = LoggerFactory.getLogger("producer");
    private org.apache.kafka.clients.producer.KafkaProducer<String, String> kafkaProducer;
    private Boolean isAsync = false;
    private String app;
    private String topic;

    private void init() {
        Properties p = null;
        InputStream in = null;
        InputStream configIn = null;
        try {

            URL configUrl = KafkaProducer.class.getClassLoader().getResource("check.properties");
            if (configUrl == null) {
                configUrl = KafkaProducer.class.getClassLoader().getResource("conf/check.properties");
            }
            configIn = configUrl.openStream();
            Properties configPro = new Properties();
            configPro.load(configIn);
            app = configPro.getProperty("producer.app");
            topic = configPro.getProperty("producer.topic");


            p = new Properties();
            URL url = KafkaProducer.class.getClassLoader().getResource("kafka-producer.properties");
            if (url == null) {
                url = KafkaProducer.class.getClassLoader().getResource("conf/kafka-producer.properties");
            }
            in = url.openStream();
            p.load(in);

            kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(p);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (configIn != null) {
                try {
                    configIn.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void open() throws Exception {
        init();
    }

    @Override
    public void close() throws Exception {
        kafkaProducer.close();
    }

    @Override
    public void send(String message) throws Exception {
        long startTime = System.currentTimeMillis();
        if (isAsync) { // Send asynchronously
            kafkaProducer.send(new ProducerRecord<String, String>(topic,
                    message,
                    message), new DemoCallBack(startTime, message, message));
            logger.info("async sent message: (" + message + ", " +  message + ")");
        } else { // Send synchronously
            try {
                kafkaProducer.send(new ProducerRecord<String, String>(topic,
                        message,
                        message)).get();
//                Thread.sleep(100);
                logger.info("sync sent message: (" + message + ", " + message + ")");
            } catch (InterruptedException e) {
                e.printStackTrace();
                throw e;
            } catch (ExecutionException e) {
                e.printStackTrace();
                throw e;
            }
        }
    }

    class DemoCallBack implements Callback {

        private long startTime;
        private String key;
        private String message;

        public DemoCallBack(long startTime, String key, String message) {
            this.startTime = startTime;
            this.key = key;
            this.message = message;
        }

        /**
         * A callback method the user can implement to provide asynchronous handling of request completion. This method will
         * be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
         * non-null.
         *
         * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
         *                  occurred.
         * @param exception The exception thrown during processing of this record. Null if no error occurred.
         */
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (metadata != null) {
                System.out.println(
                        "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
                                "), " +
                                "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
            } else {
                exception.printStackTrace();
            }
        }
    }
}
