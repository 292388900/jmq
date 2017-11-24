package com.ipd.jmq.test.performance.producer.kafka;

import com.ipd.jmq.test.performance.producer.Producer;
import com.ipd.jmq.test.performance.producer.ProducerStat;
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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by zhangkepeng on 16-7-11.
 */
public class KafkaProducer implements Producer {
    private final static Logger logger = LoggerFactory.getLogger(KafkaProducer.class);

    private AtomicLong id = new AtomicLong(0);
    private org.apache.kafka.clients.producer.KafkaProducer kafkaProducer;
    private Boolean isAsync = false;

    public KafkaProducer(Boolean isAsync) {

        this.isAsync = isAsync;
    }

    @Override
    public void init() {
        Properties props = new Properties();
        InputStream in = null;
        try {
            URL url = KafkaProducer.class.getClassLoader().getResource("kafka-producer.properties");
            if (url == null) {
                url = KafkaProducer.class.getClassLoader().getResource("conf/kafka-producer.properties");
            }
            in = url.openStream();
            props.load(in);
        } catch (Exception e) {
            logger.error("",e);
        }finally {
            if(in != null){
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer(props);
    }

    @Override
    public void send(final String topic,final String text) {
        long seq = id.incrementAndGet();
        long startTime = System.currentTimeMillis();
        if (isAsync) { // Send asynchronously
            kafkaProducer.send(new ProducerRecord<String, String>(topic,
                    String.valueOf(seq),
                    text + seq), new DemoCallBack(startTime, seq, text));
//            logger.info("async sent message: (" + seq + ", " + text + seq + ")");
        } else { // Send synchronously
            try {
                kafkaProducer.send(new ProducerRecord<String, String>(topic,
                        String.valueOf(seq),
                        text + seq)).get();
//                logger.info("sync sent message: (" + seq + ", " + text + seq + ")");
            } catch (InterruptedException e) {
                e.printStackTrace();
                ProducerStat.tpStatBuffer.error();
                ProducerStat.errs.incrementAndGet();
            } catch (ExecutionException e) {
                e.printStackTrace();
                ProducerStat.tpStatBuffer.error();
                ProducerStat.errs.incrementAndGet();
            }
        }
    }
}

class DemoCallBack implements Callback {

    private long startTime;
    private long key;
    private String message;

    public DemoCallBack(long startTime, long key, String message) {
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
//            System.out.println(
//                    "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
//                            "), " +
//                            "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        } else {
            exception.printStackTrace();
        }
    }
}
