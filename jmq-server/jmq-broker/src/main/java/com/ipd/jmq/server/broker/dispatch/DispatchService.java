package com.ipd.jmq.server.broker.dispatch;

import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.BrokerMessage;
import com.ipd.jmq.common.message.MessageLocation;
import com.ipd.jmq.common.network.v3.session.Consumer;
import com.ipd.jmq.server.broker.monitor.BrokerMonitor;
import com.ipd.jmq.server.broker.offset.OffsetBlock;
import com.ipd.jmq.server.broker.offset.TopicOffset;
import com.ipd.jmq.server.broker.offset.UnSequenceOffset;
import com.ipd.jmq.registry.listener.LeaderListener;
import com.ipd.jmq.toolkit.concurrent.EventListener;
import com.ipd.jmq.toolkit.lang.LifeCycle;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * 分发服务
 */
public interface DispatchService extends LifeCycle, LeaderListener {

    /**
     * 取得消息
     *
     * @param consumer
     * @param count
     * @param ackTimeout
     * @param queueId    默认值0
     * @param offset     默认值-1
     * @return
     * @throws JMQException
     */
    PullResult getMessage(Consumer consumer, int count, int ackTimeout, int queueId, long offset) throws JMQException;

    /**
     * 增加消费者错误
     *
     * @param consumerId
     * @param isExpired
     * @return
     */
    ConsumerErrStat addConsumeErr(String consumerId, boolean isExpired);


    /**
     * 消费连续错误是否需要暂停
     *
     * @param consumerId
     * @param timeout    确认超时时间
     * @return
     */
    boolean needPause(String consumerId, long timeout) throws JMQException;

    //清理消费者连续错误记录
    void cleanConsumeErr(String consumerId);

    void setBrokerMonitor(BrokerMonitor brokerMonitor);

    /**
     * 是否有空闲的有未拉取数据的队列，提供给消费者消费
     *
     * @param consumer 消费者
     * @return <li>true 有空闲队列</li>
     * <li>false 无空闲队列</li>
     */
    boolean hasFreeQueue(Consumer consumer) throws JMQException;

    /**
     * 应答消息，不包含重试消息
     *
     * @param locations    应答消息
     * @param consumer     消费者
     * @param isSuccessAck 是否正常确认
     * @return 是否成功
     * @throws com.ipd.jmq.common.exception.JMQException
     */
    boolean acknowledge(MessageLocation[] locations, Consumer consumer, boolean isSuccessAck) throws JMQException;

    /**
     * 清理过期数据
     *
     * @param consumer 消费者
     * @param queueId  队列ID
     */
    void cleanExpire(Consumer consumer, int queueId);

    /**
     * 清理过期数据
     *
     * @param topic   主题
     * @param app     消费者
     * @param queueId 队列ID
     */
    boolean cleanExpire(String topic, String app, int queueId);

    /**
     * 清理过期数据
     *
     * @param topic     主题
     * @param app       消费者
     * @param queueId   队列ID
     * @param ownerShip 所有权
     */
    boolean cleanExpire(String topic, String app, int queueId, OwnerShip ownerShip);

    /**
     * 获取location的信息
     *
     * @param topic
     * @param app
     * @return
     */
    List<String> getLocations(String topic, String app);

    /**
     * 获取leader
     *
     * @return
     */
    ConcurrentMap getLeaders();


    /**
     * 判断消息是否积压
     *
     * @param topic 主题
     * @param app   消费者
     * @return
     */
    boolean isPendingExcludeRetry(String topic, String app);

    /**
     * 添加派发监听器
     *
     * @param eventListener 监听器
     * @return
     */
    boolean addListener(EventListener<DispatchEvent> eventListener);

    /**
     * 获取当前BROKER没有确认的区间
     *
     * @return
     */
    Map<ConcurrentPull.PullStatKey, List<OffsetBlock>> getNoAckBlocks();

    /**
     * 根据时间戳或偏移量回溯消费位置
     *
     * @param topic    主题
     * @param queueId  队列号
     * @param consumer 应用
     * @param offset   偏移量
     * @return
     */
    void resetAckOffset(String topic, int queueId, String consumer, long offset) throws JMQException;

    /**
     * 确认消费超时的应用
     *
     * @param topic
     * @param consumer
     */
    void ackConsumerExpiredMessage(String topic, String consumer) throws JMQException;


    /**
     * 获取并行消费偏移量
     *
     * @param topic
     * @param app
     * @param queueId
     * @return
     */
    UnSequenceOffset getUnSequenceOffset(String topic, String app, int queueId);

    /**
     * 开始消费位置
     * 必须为订阅后的consumer才能调用
     *
     * @param topic    主题
     * @param queueId  队列序号
     * @param consumer 消费者
     * @return 开始消费位置
     */
    long getOffset(String topic, int queueId, String consumer);

    /**
     * 获取主题的消费位置
     *
     * @param topic 主题
     * @return 消费位置
     */
    TopicOffset getOffset(String topic);

    /**
     * 获取积压数
     *
     * @param topic    主题
     * @param consumer 消费者
     * @return 积压数
     */
    long getPending(String topic, String consumer);

    /**
     * 订阅主题
     *
     * @param topic    主题
     * @param consumer 应用
     */
    void subscribe(String topic, String consumer);


    /**
     * 取消订阅
     *
     * @param topic
     * @param consumer
     */
    void unSubscribe(final String topic, final String consumer);


    /**
     * 指定队列
     *
     * @param message
     * @return
     * @throws Exception
     */
    BrokerMessage dispatchQueue(final BrokerMessage message) throws Exception;

}
