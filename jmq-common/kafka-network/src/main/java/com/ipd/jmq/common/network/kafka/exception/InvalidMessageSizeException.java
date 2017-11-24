package com.ipd.jmq.common.network.kafka.exception;

/**
 * Created by zhangkepeng on 16-9-12.
 */
public class InvalidMessageSizeException extends RuntimeException{


    public InvalidMessageSizeException(String message) {
        super(message);
    }

    public InvalidMessageSizeException(String message, Throwable e) {
        super(message, e);
    }
}
