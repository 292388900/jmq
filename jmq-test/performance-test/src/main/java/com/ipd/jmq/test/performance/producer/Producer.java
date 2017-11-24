package com.ipd.jmq.test.performance.producer;

/**
 * Created by zhangkepeng on 17-1-6.
 */
public interface Producer {

    void init();

    void send(final String topic, final String text);
}
