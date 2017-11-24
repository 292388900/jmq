package com.ipd.jmq.common.network.kafka.exception;

/**
 * Created by zhangkepeng on 16-9-1.
 */
public class InvalidMessageException extends RuntimeException {

    public InvalidMessageException(String message) {
        super(message);
    }

    public InvalidMessageException(String message, Throwable e) {
        super(message, e);
    }
}
