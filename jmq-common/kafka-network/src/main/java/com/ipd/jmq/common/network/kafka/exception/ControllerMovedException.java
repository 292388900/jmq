package com.ipd.jmq.common.network.kafka.exception;

/**
 * Created by zhangkepeng on 16-9-13.
 */
public class ControllerMovedException extends RuntimeException {

    public ControllerMovedException(String message) {
        super(message);
    }

    public ControllerMovedException(String message, Throwable e) {
        super(message, e);
    }
}
