package com.ipd.jmq.server.store;

import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.*;
import com.ipd.jmq.common.model.JournalLog;
import com.ipd.jmq.server.store.journal.FileStat;
import com.ipd.jmq.toolkit.buffer.RByteBuffer;
import com.ipd.jmq.toolkit.lang.LifeCycle;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * 存储接口
 */
public interface Store extends LifeCycle {
    interface ProcessedCallback<T extends JournalLog> {
        void onProcessed(StoreContext<T> context) throws JMQException;
    }

    /**
     * 返回队列最大偏移量
     *
     * @param topic   主题
     * @param queueId 队列序号
     * @return 队列最大偏移量
     */
    long getMaxOffset(String topic, int queueId);

    /**
     * 返回队列最小偏移量
     *
     * @param topic   主题
     * @param queueId 队列序号
     * @return 队列最小偏移量
     */
    long getMinOffset(String topic, int queueId);

    /**
     * 返回日志最大偏移量
     *
     * @return 日志最大偏移量
     */
    long getMaxOffset();

    /**
     * 返回日志最小偏移量
     *
     * @return 日志最小偏移量
     */
    long getMinOffset();


    /**
     * 获取下一条消息的偏移量
     *
     * @param topic   主题
     * @param queueId 队列序号
     * @param offset  当前偏移量
     * @return 下一条消息偏移量
     */
    long getNextOffset(String topic, int queueId, long offset);

    /**
     * 获取单条消息
     *
     * @param topic       主题
     * @param queueId     队列序号
     * @param queueOffset 队列偏移量
     * @return 单条消息
     * @throws com.ipd.jmq.common.exception.JMQException
     */
    RByteBuffer getMessage(final String topic, final int queueId, final long queueOffset) throws
            JMQException;

    /**
     * 获取队列信息
     *
     * @param topic       主题
     * @param queueId     队列ID
     * @param queueOffset 队列偏移量
     * @return 索引信息
     * @throws JMQException
     */
    QueueItem getQueueItem(final String topic, final int queueId, final long queueOffset) throws JMQException;

    /**
     * 获取最小队列信息
     *
     * @param topic     主题
     * @param queueId   队列ID
     * @param minOffset 最小偏移量
     * @param maxOffset 最大偏移量
     * @return 索引信息(大于等于minOffset, 小于等于maxOffset)
     * @throws JMQException
     */
    QueueItem getMinQueueItem(final String topic, int queueId, long minOffset, long maxOffset) throws JMQException;

    /**
     * 根据时间获取队列偏移量
     * <p/>
     * 获取到的偏移量时间大于timestamp从timestamp开始
     *
     * @param topic     主题
     * @param queueId   队列ID
     * @param consumer  消费者
     * @param timestamp 开始时间
     * @param ackOffset 当前的确认位置
     * @return
     * @throws JMQException
     */
    long getQueueOffsetByTimestamp(String topic, int queueId, String consumer, long timestamp, long ackOffset)
            throws JMQException;

    /**
     * 获取timestamp时间点之前的队列位置
     *
     * @param topic     主题
     * @param queueId   队列ID
     * @param timestamp 时间戳
     * @param maxCount  最大数量
     * @return
     * @throws JMQException
     */
    Set<Long> getQueueOffsetsBefore(String topic, int queueId, long timestamp, int maxCount) throws JMQException;

    /**
     * 获取消息
     * 如果碰到索引过期等情况，会跳过，接着从后续索引中获取数据，不适合按指定位置获取消息的场景
     *
     * @param topic        主题
     * @param queueId      队列序号
     * @param queueOffset  队列偏移量
     * @param count        数量
     * @param maxFrameSize 网络数据包大小
     * @return
     * @throws com.ipd.jmq.common.exception.JMQException
     */
    GetResult getMessage(String topic, int queueId, long queueOffset, int count, long maxFrameSize) throws
            JMQException;

    /**
     * 获取延时消息
     *
     * @param topic        主题
     * @param queueId      队列序号
     * @param queueOffset  队列偏移量
     * @param count        数量
     * @param maxFrameSize 网络数据包大小
     * @param delay        延迟时间(ms)
     * @return
     * @throws JMQException
     */
    GetResult getMessage(String topic, int queueId, long queueOffset, int count, long maxFrameSize, int delay) throws
            JMQException;

    /**
     * 返回所有消息队列<topic,<queueId,ConsumeQueue>>
     *
     * @return 所有消息队列
     */
    ConcurrentMap<String, List<Integer>> getQueues();

    /**
     * 返回指定主题的队列
     *
     * @param topic 主题
     * @return
     */
    List<Integer> getQueues(String topic);

    /**
     * 更新队列数
     *
     * @param topic  主题
     * @param queues 队列数量
     * @return
     * @throws JMQException
     */
    List<Integer> updateQueues(String topic, int queues) throws JMQException;

    /**
     * 增加队列
     *
     * @param topic 主题
     * @param queueId 队列ID
     * @throws JMQException
     */
    void addQueue(String topic, short queueId) throws JMQException;



    /**
     * 是否磁盘已写满
     *
     * @return 磁盘写满标示
     */
    boolean isDiskFull();

    /**
     * 获取磁盘使用统计信息
     *
     * @return 磁盘使用统计信息
     */
    FileStat getDiskFileStat();

    /**
     * 获取存储配置
     *
     * @return 存储配置
     */
    StoreConfig getConfig();

    /**
     * 是否有数据坏块
     *
     * @return 有坏块标示
     */
    boolean hasBadBlock();

    /**
     * 存储是否已经就绪
     *
     * @return 就绪标示
     */
    boolean isReady();

    /**
     * 通知存储，将要关闭
     */
    void willStop();

    /**
     * 返回队列最小偏移量时间戳
     *
     * @param topic
     * @param queueId
     * @return
     */
    long getMinOffsetTimestamp(String topic, int queueId) throws JMQException;

    /**
     * 返回队列最大偏移量时间戳
     *
     * @param topic
     * @param queueId
     * @return
     */
    long getMaxOffsetTimestamp(String topic, int queueId) throws JMQException;


    /**
     * 批量添加消息
     *
     * @param messages 消息集合
     * @return
     * @throws JMQException
     */
    PutResult putMessages(final List<BrokerMessage> messages, final ProcessedCallback callback) throws JMQException;

    /**
     * 开启事务
     *
     * @param prepare
     * @return
     * @throws JMQException
     */
    PutResult beginTransaction(BrokerPrepare prepare) throws JMQException;

    /**
     * 添加事务消息
     *
     * @param preMessages
     * @return
     * @throws JMQException
     */
    PutResult putTransactionMessages(List<BrokerMessage> preMessages) throws JMQException;

    /**
     * 提交事务
     *
     * @param commit
     * @return
     * @throws JMQException
     */
    PutResult commitTransaction(BrokerCommit commit) throws JMQException;

    /**
     * 回滚事务
     *
     * @param rollback
     * @return
     * @throws JMQException
     */
    PutResult rollbackTransaction(BrokerRollback rollback) throws JMQException;

    /**
     * 批量添加日志
     *
     * @param context 日志上下文
     * @return
     * @throws JMQException
     */
    void putJournalLogs(StoreContext<?> context) throws JMQException;


    /**
     * 添加日志
     *
     * @param log 日志信息
     * @param <T> 日志类型
     * @return
     * @throws JMQException
     */
    <T extends JournalLog> PutResult putJournalLog(T log) throws JMQException;

    /**
     * 获取事务准备消息
     *
     * @param txId 事务ID
     * @return
     */
    BrokerPrepare getBrokerPrepare(String txId);

    /**
     * 解锁未提交的事务
     *
     * @param txId
     */
    void unLockInflightTransaction(String txId);

    /**
     * 事务补偿回调
     *
     * @param txId
     */
    void onTransactionFeedback(String txId);

    /**
     * 获取存储启动时间
     *
     * @return
     */
    int getRestartSuccess();

    /**
     * 获取重启次数
     *
     * @return
     */
    int getRestartTimes();


    /**
     * 锁定并获取未提交的事务
     *
     * @param topic
     * @param owner
     * @return
     * @throws JMQException
     */
    GetTxResult getInflightTransaction(String topic, String owner) throws JMQException;

    // Replication使用的数据块读写接口

    /**
     * 读取消息块数据
     *
     * @param offset 日志文件起始偏移量
     * @param size   消息块长度
     * @return 消息块数据对象
     * @throws Exception
     */
    RByteBuffer readMessageBlock(long offset, int size) throws Exception;

    /**
     * 写入消息块数据
     *
     * @param offset     日志文件起始偏移量
     * @param buffer     要写入的数据
     * @param buildIndex true=为写入的消息块建立队列索引
     * @throws Exception
     */
    void writeMessageBlock(long offset, RByteBuffer buffer, boolean buildIndex) throws Exception;

    /**
     * 清理存储
     *
     * @param minAckOffsets 最小队列偏移量
     * @throws Exception
     */
    void cleanUp(Map<String, Long> minAckOffsets) throws Exception;
}