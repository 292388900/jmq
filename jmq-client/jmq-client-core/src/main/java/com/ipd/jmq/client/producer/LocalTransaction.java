package com.ipd.jmq.client.producer;

/**
 * 本地事务
 */
public interface LocalTransaction<T> {

    /**
     * 本地方法执行，成功后自动提交消息事务，抛出异常自动回滚消息事务
     *
     * @return
     * @throws java.lang.Exception
     */

    T execute() throws Exception;

    /**
     * 获取超时时间
     *
     * @return 超时时间
     */
    int getTimeout();

}