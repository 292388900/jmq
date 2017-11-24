package com.ipd.jmq.server.broker.retry;

/**
 * 重试管理器类型
 */
public enum RetryManagerType {
    /**
     * 直连数据库
     */
    DB,
    /**
     * 本地不能访问数据库，调用远端服务
     */
    REMOTE

}
