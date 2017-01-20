package com.ipd.jmq.common.model;

/**
 * 乐观锁
 * Created by hexiaofeng on 15-7-16.
 */
public interface OptimisticLock {
    /**
     * 返回版本
     *
     * @return 当前版本
     */
    long getVersion();

    /**
     * 设置版本
     *
     * @param version 版本号
     */
    void setVersion(long version);

}
