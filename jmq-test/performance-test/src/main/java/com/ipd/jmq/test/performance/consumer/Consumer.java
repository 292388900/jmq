package com.ipd.jmq.test.performance.consumer;

/**
 * Created by zhangkepeng on 17-1-6.
 */
public interface Consumer {

    void init();

    void run();

    long getCountValue();

}
