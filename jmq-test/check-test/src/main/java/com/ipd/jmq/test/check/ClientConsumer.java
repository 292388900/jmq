package com.ipd.jmq.test.check;

/**
 * Created by dingjun on 15-11-23.
 */
public interface ClientConsumer {
    void open() throws Exception;
    void consume() throws Exception;
    void close() throws Exception;
}
