package com.ipd.jmq.server.store.journal;

import com.ipd.jmq.toolkit.time.Period;

import java.util.concurrent.CountDownLatch;

/**
 * 提交的数据
 * Created by hexiaofeng on 16-7-16.
 */
public interface CommitRequest {

    /**
     * 日志位置
     *
     * @return 日志位置
     */
    long getOffset();

    /**
     * 获取写入时间片段
     *
     * @return 写入时间片段
     */
    Period getWrite();

    /**
     * 数据大小
     *
     * @return 数据大小
     */
    int size();

    /**
     * 返回栅栏
     *
     * @return 栅栏
     */
    CountDownLatch getLatch();

    /**
     * 设置异常
     *
     * @param e 异常
     */
    void setException(Throwable e);
}
