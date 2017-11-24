package com.ipd.jmq.common.network.kafka.exception;

/**
 * Created by zhangkepeng on 16-8-30.
 */
public class UnknownCodecException extends RuntimeException {

    public UnknownCodecException(String message) {
        super(message);
    }

    public UnknownCodecException(String message, Throwable e) {
        super(message, e);
    }
}
