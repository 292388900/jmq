package com.ipd.jmq.test.check;

/**
 * Created by dingjun on 15-11-23.
 */
public interface ClientProducer {
    void send(String content) throws Exception;
    void open() throws Exception;
    void close() throws Exception;
}
