package com.ipd.jmq.common.network.kafka.exception;

/**
 * Created by zhangkepeng on 16-9-12.
 */
public class MessageSetSizeTooLargeException extends RuntimeException{

    public MessageSetSizeTooLargeException(String message) {
        super(message);
    }

    public MessageSetSizeTooLargeException(String messge, Throwable e) {
        super(messge, e);
    }
}
