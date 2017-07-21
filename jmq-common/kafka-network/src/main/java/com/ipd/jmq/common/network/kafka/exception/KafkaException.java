package com.ipd.jmq.common.network.kafka.exception;

/**
 * Created by zhangkepeng on 16-8-1.
 */
public class KafkaException extends RuntimeException {

    public KafkaException(String message) {
        super(message);
    }

    public KafkaException(String message, Throwable e) {
        super(message, e);
    }
}
