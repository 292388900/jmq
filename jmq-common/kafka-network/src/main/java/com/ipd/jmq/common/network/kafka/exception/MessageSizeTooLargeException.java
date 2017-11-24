package com.ipd.jmq.common.network.kafka.exception;

/**
 * Created by zhangkepeng on 16-9-1.
 */
public class MessageSizeTooLargeException extends RuntimeException {

    public MessageSizeTooLargeException(String message) {
        super(message);
    }

    public MessageSizeTooLargeException(String message, Throwable e) {
        super(message, e);
    }
}
