package com.ipd.jmq.server.broker.retry;

import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.BrokerMessage;
import com.ipd.jmq.registry.listener.LeaderListener;
import com.ipd.jmq.toolkit.lang.LifeCycle;

import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * 重试管理器
 */
public interface RetryManager extends LifeCycle {

    /**
     * 增加重试
     *
     * @param topic     主题
     * @param app       应用
     * @param messages  消息
     * @param exception 异常
     * @throws JMQException
     */
    void addRetry(String topic, String app, BrokerMessage[] messages, String exception) throws JMQException;

    /**
     * 更新重试消息状态到重试成功
     *
     * @param topic      主题
     * @param app        应用
     * @param messageIds 消息
     * @throws JMQException 操作失败时
     */
    void retrySuccess(String topic, String app, long[] messageIds) throws JMQException;

    /**
     * 更新重试消息状态到重试错误
     *
     * @param topic      主题
     * @param app        应用
     * @param messageIds 消息
     * @throws JMQException 操作失败时
     */
    void retryError(String topic, String app, long[] messageIds) throws JMQException;

    /**
     * 更新重试消息状态为重试过期
     *
     * @param topic      主题
     * @param app        应用
     * @param messageIds 消息
     * @throws JMQException 操作失败时
     */
    void retryExpire(String topic, String app, long[] messageIds) throws JMQException;

    /**
     * 查询指定主题和个数的重试消息
     * <p/>
     * <p>
     * 该接口要求实现类返回非空List, 即要么返回非空List，要么抛出异常
     * </p>
     *
     * @param topic   主题
     * @param app     应用
     * @param count   条数
     * @param startId 起始ID
     */
    List<BrokerMessage> getRetry(String topic, String app, short count, long startId) throws JMQException;

    /**
     * 获取重试数据量
     *
     * @param topic 主题
     * @param app   应用
     * @return 重试数据量
     * @throws JMQException
     */
    int getRetry(String topic, String app) throws JMQException;

    /**
     * 清空缓存
     * @param topic 主题
     * @param app 应用
     */
    public void clearCache(final String topic, final String app);

    /**
     * 查看缓存的消息
     * @param topic 主题
     * @param app 应用
     * @return
     */
    public BlockingQueue<BrokerMessage> viewCachedRetry(final String topic, final String app);

    /**
     * 重新加载数据到缓存中
     *
     * @param topic
     * @param app
     */
    public void reloadRetryCache(final String topic, final String app);

    /**
     * 设置选举监听器
     *
     * @param leaderListener 选举监听器
     */
    void setLeaderListener(LeaderListener leaderListener);

}