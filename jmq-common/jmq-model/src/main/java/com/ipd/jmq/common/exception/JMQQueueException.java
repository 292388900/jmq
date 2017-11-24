package com.ipd.jmq.common.exception;

/**
 * Created by dingjun on 14-8-26.
 */
public class JMQQueueException extends JMQException{


    public JMQQueueException(String message) {
        super(message, JMQCode.SE_INVALID_JOURNAL.getCode());
    }
}
