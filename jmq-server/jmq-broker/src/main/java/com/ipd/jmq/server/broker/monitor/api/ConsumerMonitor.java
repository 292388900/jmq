package com.ipd.jmq.server.broker.monitor.api;

import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.BrokerMessage;
import com.ipd.jmq.common.model.OffsetInfo;

import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Created by zhangkepeng on 16-11-30.
 */
public interface ConsumerMonitor {

    /**
     * 增加消费者
     *
     * @param topic 主题
     * @param app   应用
     */
    void subscribe(String topic, String app);

    /**
     * 移除消费者
     *
     * @param topic
     * @param consumer
     */
    void unsubscribe(final String topic, final String consumer);

    /**
     * 查看消息
     *
     * @param topic 主题
     * @param app   应用
     * @param count 最大消息条数
     */
    List<BrokerMessage> viewMessage(String topic, String app, int count);

    /**
     * 查看最小消费位置
     *
     * @param topic 队列
     * @param app 消费者
     * @return
     */
    OffsetInfo getOffsetInfo(String topic, short queueId, String app);

    /**
     * 根据时间戳或偏移量回溯消费位置
     *
     * @param topic 主题
     * @param queueId 队列号
     * @param app 应用
     * @param offsetValueOrTimeStamp 回溯消费位置或时间戳
     * @param offsetValueOrTimeStampBoolean true:offsetValueOrTimeStamp回溯消费位置;false:offsetValueOrTimeStamp回溯消费时间
     */
    void resetAckOffset(String topic, short queueId, String app, long offsetValueOrTimeStamp, boolean offsetValueOrTimeStampBoolean) throws JMQException;

    /**
     * 没有确认的索引块
     * @return
     */
    String getNoAckBlocks();

    /**
     * 获取location信息
     *
     * @param topic 主题
     * @param app   应用
     * @return 消费位置列表
     */
    List<String> getLocations(String topic, String app);

    /**
     * 清理过期数据
     *
     * @param topic   主题
     * @param app     消费者
     * @param queueId 队列ID
     */
    boolean cleanExpire(String topic, String app, short queueId);

    /**
     * 清理重试缓存数据
     *
     * @param topic 主题
     * @param app   应用
     */
    void clearRetryCache(String topic, String app);

    /**
     * 查看缓存的重试数据
     *
     * @param topic 主题
     * @param app   应用
     */
    BlockingQueue<BrokerMessage> viewCachedRetry(String topic, String app);

    /**
     * 重新加载重试缓存数据
     *
     * @param topic
     * @param app
     */
    void reloadRetryCache(String topic, String app);

    /**
     * 是否是重试leader
     *
     * @param topic
     * @param app
     * @return
     */
    boolean isRetryLeader(String topic, String app);
}
