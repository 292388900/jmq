package com.ipd.jmq.server.broker.dispatch;

import com.ipd.jmq.common.cluster.TopicConfig;
import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.BrokerMessage;
import com.ipd.jmq.common.message.MessageLocation;
import com.ipd.jmq.common.message.MessageQueue;
import com.ipd.jmq.common.message.QueueItem;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.v3.netty.NettyDecoder;
import com.ipd.jmq.common.network.v3.session.Consumer;
import com.ipd.jmq.common.network.v3.session.Joint;
import com.ipd.jmq.common.network.v3.session.Producer;
import com.ipd.jmq.server.broker.BrokerConfig;
import com.ipd.jmq.server.broker.SessionManager;
import com.ipd.jmq.server.broker.cluster.ClusterEvent;
import com.ipd.jmq.server.broker.cluster.ClusterManager;
import com.ipd.jmq.server.broker.cluster.TopicUpdateEvent;
import com.ipd.jmq.server.broker.monitor.BrokerMonitor;
import com.ipd.jmq.server.broker.retry.RetryManager;
import com.ipd.jmq.server.broker.sequence.Sequence;
import com.ipd.jmq.server.store.ConsumeQueue;
import com.ipd.jmq.server.store.GetResult;
import com.ipd.jmq.server.store.MinTimeOffset;
import com.ipd.jmq.server.store.Store;
import com.ipd.jmq.server.broker.offset.*;
import com.ipd.jmq.registry.listener.LeaderEvent;
import com.ipd.jmq.registry.util.Path;
import com.ipd.jmq.toolkit.buffer.RByteBuffer;
import com.ipd.jmq.toolkit.concurrent.EventBus;
import com.ipd.jmq.toolkit.concurrent.EventListener;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.service.ServiceThread;
import com.ipd.jmq.toolkit.time.SystemClock;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 派发服务
 */
public class DispatchManager extends Service implements DispatchService {
    private static Logger logger = LoggerFactory.getLogger(DispatchManager.class);
    protected BrokerConfig config;
    // session管理。
    protected SessionManager sessionManager;
    // 存储接口
    protected Store store;
    // 集群管理
    protected ClusterManager clusterManager;
    // 获取重试数据。
    protected RetryManager retryManager;
    // 会话监听器
    protected EventListener<SessionManager.SessionEvent> sessionListener;
    // 集群监听器
    protected EventListener<ClusterEvent> clusterListener;
    // 主题映射
    protected ConcurrentMap<String, DispatchTopic> topics = new ConcurrentHashMap<String, DispatchTopic>();
    // 重试Leader
    protected ConcurrentMap<Joint, Boolean> leaders = new ConcurrentHashMap<Joint, Boolean>();
    // 清理线程，检查过期及待删除的队列是否可以删除
    protected Thread cleanThread;
    // 消费者请求消息的次数Map,用于实现每个消费者对每个主题的队列的公平访问,访问策略用轮询实现
    private ConcurrentMap<Joint, AtomicLong> counters = new ConcurrentHashMap<Joint, AtomicLong>();
    // 主题监听器
    private TopicListener topicListener = new TopicListener();
    // 服务监控
    private BrokerMonitor brokerMonitor;
    // 连续发生retry或ack expired
    private ConcurrentMap<String, ConsumerErrStat> errStats = new ConcurrentHashMap<String, ConsumerErrStat>();
    //
    private EventBus<DispatchEvent> dispatchEventManager = new EventBus<DispatchEvent>();

    private ConcurrentPull concurrentPull;

    private OffsetManager offsetManager;

    protected static int DEFAULT_BLOCK_EXPIRED = 1500;
    public DispatchManager(){
        concurrentPull = new ConcurrentPull();
        concurrentPull.setDispatchManager(this);
        this.clusterListener = new EventListener<ClusterEvent>() {
            @Override
            public void onEvent(ClusterEvent event) {
                if (isStarted()) {
                    try {
                        switch (event.getType()) {
                            case TOPIC_UPDATE:
                                TopicUpdateEvent topicNotify = (TopicUpdateEvent) event;
                                onQueueChange(topicNotify.getTopicConfig().getTopic(),
                                        topicNotify.getTopicConfig().getQueues());
                                break;
                        }
                    } catch (JMQException e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }
        };
        this.sessionListener = new EventListener<SessionManager.SessionEvent>() {
            @Override
            public void onEvent(SessionManager.SessionEvent event) {
                if (isStarted()) {
                    try {
                        switch (event.getType()) {
                            case AddConsumer:
                                onAddConsumer(event.getConsumer());
                                break;
                            case RemoveConsumer:
                                onRemoveConsumer(event.getConsumer());
                                break;
                            case AddProducer:
                                onAddProducer(event.getProducer());
                                break;
                        }
                    } catch (JMQException e) {
                        if (isStarted()) {
                            logger.error(e.getMessage());
                        }
                    }
                }
            }
        };

    }
    public DispatchManager(SessionManager sessionManager, final ClusterManager clusterManager, RetryManager retryManager,
                           BrokerConfig config, OffsetManager offsetManager) {
        if (sessionManager == null) {
            throw new IllegalArgumentException("retryManager can not be null");
        }
        if (clusterManager == null) {
            throw new IllegalArgumentException("clusterManager can not be null");
        }
        if (retryManager == null) {
            throw new IllegalArgumentException("retryManager can not be null");
        }
        if (config == null) {
            throw new IllegalArgumentException("config can not be null");
        }
        if (offsetManager == null){
            throw new IllegalArgumentException("offsetManager can not be null");
        }

        this.config = config;
        this.store = config.getStore();
        this.retryManager = retryManager;
        this.retryManager.setLeaderListener(this);
        this.sessionManager = sessionManager;
        this.clusterManager = clusterManager;
        this.offsetManager = offsetManager;
        this.clusterListener = new EventListener<ClusterEvent>() {
            @Override
            public void onEvent(ClusterEvent event) {
                if (isStarted()) {
                    try {
                        switch (event.getType()) {
                            case TOPIC_UPDATE:
                                TopicUpdateEvent topicNotify = (TopicUpdateEvent) event;
                                onQueueChange(topicNotify.getTopicConfig().getTopic(),
                                        topicNotify.getTopicConfig().getQueues());
                                break;
                        }
                    } catch (JMQException e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }
        };
        this.sessionListener = new EventListener<SessionManager.SessionEvent>() {
            @Override
            public void onEvent(SessionManager.SessionEvent event) {
                if (isStarted()) {
                    try {
                        switch (event.getType()) {
                            case AddConsumer:
                                onAddConsumer(event.getConsumer());
                                break;
                            case RemoveConsumer:
                                onRemoveConsumer(event.getConsumer());
                                break;
                            case AddProducer:
                                onAddProducer(event.getProducer());
                                break;
                        }
                    } catch (JMQException e) {
                        if (isStarted()) {
                            logger.error(e.getMessage());
                        }
                    }
                }
            }
        };

        concurrentPull = new ConcurrentPull(this, config, offsetManager);
    }

    public void setBrokerMonitor(BrokerMonitor brokerMonitor) {
        this.brokerMonitor = brokerMonitor;
    }
    public void setConfig(BrokerConfig config) {
        this.config = config;
        this.store = config.getStore();
        concurrentPull.setBrokerConfig(config);
    }

    public void setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    public void setClusterManager(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    public void setRetryManager(RetryManager retryManager) {
        this.retryManager = retryManager;
        this.retryManager.setLeaderListener(this);
    }

    public void setOffsetManager(OffsetManager offsetManager) {
        this.offsetManager = offsetManager;
        concurrentPull.setOffsetManager(offsetManager);
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        // 清理应答过期线程和顺序消息派发列表中不存在消费者
        cleanThread = new Thread(new ServiceThread(this) {
            @Override
            protected void execute() throws Exception {
                cleanExpire();
                sinkSequentialQueues();
            }

            @Override
            public long getInterval() {
                return config.getCheckAckExpireInterval();
            }

            @Override
            public boolean onException(Throwable e) {
                logger.error(e.getMessage(), e);
                return true;
            }
        }, "JMQ_SERVER_ACK_EXPIRE_CLEANER");

        cleanThread.setDaemon(true);
        cleanThread.start();
        // 从存储恢复所有队列
        topics.clear();
        restoreQueues();
        // 添加会话监听器
        sessionManager.addListener(sessionListener);
        // 添加集群监听器
        clusterManager.addListener(clusterListener);
        // 添加TOPIC监控
        clusterManager.addListener(getTopicListener());
        dispatchEventManager.start();
        concurrentPull.start();
        concurrentPull.addSessionListener(sessionManager);
        logger.info("dispatch manager is started");
    }

    @Override
    protected void doStop() {
        super.doStop();
        // 移除监听器
        sessionManager.removeListener(sessionListener);
        clusterManager.removeListener(clusterListener);
        // 添加TOPIC监控
        clusterManager.removeListener(getTopicListener());
        concurrentPull.stop();
        // 停止应答过期检查线程
        if (cleanThread != null) {
            cleanThread.interrupt();
        }
        cleanThread = null;
        logger.info("dispatch manager is stopped");
    }

    @Override
    public ConsumerErrStat addConsumeErr(String consumerId, boolean isExpired) {
        ConsumerErrStat cur = new ConsumerErrStat(consumerId);
        ConsumerErrStat old = errStats.putIfAbsent(consumerId, cur);
        if (old != null) {
            cur = old;
        }
        if (isExpired) {
            cur.incrementExpired();
        } else {
            cur.incrementErr();
        }
        if (logger.isDebugEnabled()) {
            logger.debug("add err" + consumerId);
        }
        return cur;
    }

    @Override
    public boolean needPause(String consumerId, long timeout) throws JMQException {
        /**
         * 连续错误暂停当前拉取消息，如果为连续过期则暂停确认超时的时间+出错时间
         */
        ConsumerErrStat errStat = errStats.get(consumerId);
        if (errStat != null && (errStat.getCount() + errStat.getExpired() >= config.getConsumerContinueErrs())) {
            if ((errStat.getLastTimestamp() + config.getConsumerContinueErrPauseInterval() > SystemClock.now() && errStat.getExpired() <= 0)
                    || (errStat.getLastTimestamp() + config.getConsumerContinueErrPauseInterval() + timeout > SystemClock.now() && errStat.getExpired() > 0)
                    ) {
                errStat.incrementPause();
                if (logger.isDebugEnabled()) {
                    logger.debug("consumer pull pause-" + consumerId + "--errStatCount--" + errStat.getCount() + errStat.getExpired());
                }
                if (errStat.setNeedThrowErr(false)) {
                    //只第一次抛出异常
                    throw new JMQException(String.format("%s Continuous %s %d times,will pause consume time %d ms", consumerId, errStat.getExpired() > 0 ? "exceed the ackTimeout" : "errors",
                            errStat.getCount() + errStat.getExpired(),
                            errStat.getExpired() > 0 ? config.getConsumerContinueErrPauseInterval() + timeout : config.getConsumerContinueErrPauseInterval())
                            , JMQCode.CT_LIMIT_REQUEST.getCode());
                }
                return true;
            } else {
                //过了预设时间了，可以清理了
                errStats.remove(consumerId);
                logger.error("consumer continue err pause timeout:" + errStat.toString());
            }
        }
        return false;
    }

    @Override
    public void cleanConsumeErr(String consumerId) {
        errStats.remove(consumerId);
        if (logger.isDebugEnabled()) {
            logger.debug("clean consumer err:" + consumerId);
        }
    }

    /**
     * 判断一个消费者是不是锁定了太多的队列
     *
     * @param consumer
     * @param topic
     * @param policy
     * @return
     */
    private boolean lockMore(final Consumer consumer, final DispatchTopic topic, final TopicConfig.ConsumerPolicy policy, final long ackTimeout) {
        /*if (policy != null && policy.getAckTimeout() != null && policy.getAckTimeout() > 0 && policy.getAckTimeout() < config.getLimitLockMoreAckInterval()) {
            //设置了消费策略就不判断了，用于人工解除锁定
            return false;
        }*/

        List<DispatchQueue> normalQueues = topic.getNormalQueues();
        if (normalQueues.isEmpty() || normalQueues.size() <= 2) {
            return false;
        }
        int maxLocks = maxLockQueues(consumer, normalQueues.size(), ackTimeout);

        int locks = 0;
        for (DispatchQueue queue : normalQueues) {
            DispatchLocation dispatchLocation = queue.getLocation(consumer.getApp());
            if (dispatchLocation != null) {
                OwnerShip ownerShip = dispatchLocation.getOwner().get();
                if (ownerShip != null && ownerShip.getOwner().contains(consumer.getConnectionId())) {
                    locks++;
                }
            }
        }
        if (locks >= maxLocks) {
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("topic:%s,app:%s,consumer:%s lock more %d >= %d", consumer.getTopic(), consumer.getApp(), consumer.getId(), locks, maxLocks));
            }
            return true;
        }
        return false;
    }

    /**
     * 最大锁住队列数，30S会平衡计算一次，超过时间没有拉取消息的客户端会被忽略
     *
     * @param consumer
     * @param queueCount
     * @param ackTimeout
     * @return
     */
    private int maxLockQueues(Consumer consumer, int queueCount, long ackTimeout) {
        int maxLocks = 1;
        if (consumer.getRebalanceTime() == 0 || consumer.getRebalanceTime() + config.getConsumerReBalanceInterval() < SystemClock.getInstance().now()) {
            int size = sessionManager.getConsumerSize(consumer.getApp(), consumer.getTopic(), ackTimeout);
            if (size <= 1) {
                maxLocks = queueCount;
            } else {
                maxLocks = queueCount / size + 1;
            }
            if (maxLocks < 1) {
                maxLocks = 1;
            }
            consumer.setMaxLockQueues(maxLocks);
            consumer.setRebalanceTime(SystemClock.getInstance().now());
        } else {
            maxLocks = consumer.getMaxLockQueues();
        }
        if (maxLocks < 1) {
            maxLocks = 1;
        }

        return maxLocks;
    }

    /**
     * 指定队列和偏移量获取消息，不加锁，不需要客户端ack
     *
     * @param consumer
     * @param count
     * @param ackTimeout
     * @param queueId
     * @param offset
     * @return
     * @throws JMQException
     */
    @Override
    public PullResult getMessage(Consumer consumer, int count, int ackTimeout, int queueId, long offset) throws
            JMQException {
        if (consumer == null) {
            throw new JMQException(JMQCode.FW_CONSUMER_NOT_EXISTS);
        }
        Consumer.ConsumeType type = consumer.getType();
        if (!isStarted()) {
            throw new JMQException(JMQCode.CN_SERVICE_NOT_AVAILABLE);
        }

        consumer.setLastGetMessageTime(SystemClock.getInstance().now());
        List<DispatchQueue> queues = new ArrayList<DispatchQueue>();
        // 选择队列取数
        GetResult result = null;
        DispatchLocation location = null;
        OwnerShip ownerShip = new OwnerShip(consumer.getId(), 0);
        TopicConfig.ConsumerPolicy policy = null;
        // 批量大小
        int batchSize = config.getBatchSize();
        // 超时时间
        int timeout = config.getAckTimeout();
        long msgQueueOffset;
        int msgBatchSize;
        int delay = 0;
        // 获取队列
        DispatchTopic topic = getAndCreateConsumeTopic(consumer.getTopic());
        if (queueId > 0) {
            DispatchQueue curQueue = topic.getQueue(queueId);
            if (curQueue == null) {
                throw new JMQException("queueId " + queueId + " is invalid", JMQCode.CN_PARAM_ERROR.getCode());
            }
            queues.add(curQueue);
        } else {
            queues = getSortedQueues(consumer, policy, topic);
        }
        if (type.equals(Consumer.ConsumeType.JMQ)) {
            //延迟消费
            policy = clusterManager.checkReadable(consumer.getTopic(), consumer.getApp());
            if (policy.checkSequential()) {
                if (!clusterManager.checkSequentialReadable(consumer.getTopic(), consumer.getApp())) {
                    logger.warn(String.format("current group:%s is not then readable group:%s", clusterManager.getBrokerGroup().getGroup(), clusterManager.getReadableBroker(consumer.getTopic(), consumer.getApp())));
                    return null;
                }
            }
            // 批量大小
            batchSize = count <= 0 ? ((policy.getBatchSize() == null || policy.getBatchSize() <= 0) ? config
                    .getBatchSize() : policy.getBatchSize()) : count;
            // 超时时间
            timeout = ackTimeout <= 0 ? ((policy.getAckTimeout() == null || policy.getAckTimeout() <= 0) ? config
                    .getAckTimeout() : policy.getAckTimeout()) : ackTimeout;

            //延迟消费
            delay = policy.getDelay() == null ? 0 : policy.getDelay();

            if (needPause(consumer.getId(), timeout)) {
                return null;
            }
            if (!policy.checkSequential()
                    && !clusterManager.getTopicConfig(consumer.getTopic()).isLocalManageOffsetConsumer(consumer.getApp())
                    && lockMore(consumer, topic, policy, timeout)) {
                //顺序消息不做判断
                queues.clear();
                queues.add(topic.getQueue(MessageQueue.RETRY_QUEUE));
                //return null;
            }
        }

        try {
            for (DispatchQueue queue : queues) {
                OffsetBlock block = null;
                queueId = queue.getQueueId();
                // 占用，过期时间置为0，不能被清理
                if (offset >= 0 || clusterManager.getTopicConfig(consumer.getTopic()).isLocalManageOffsetConsumer(consumer.getApp())) {
                    //设置了位置，那么就由客户端控制，服务端不做控制
                    if (consumer.getType().equals(Consumer.ConsumeType.KAFKA)) {
                        location = new DispatchLocation(this, queue, consumer.getTopic());
                    } else {
                        location = new DispatchLocation(this, queue, consumer.getApp());
                    }
                    location.getOwner().set(ownerShip);
                } else if (policy.getConcurrentConsume() != null && policy.getConcurrentConsume().booleanValue() && queueId != MessageQueue.RETRY_QUEUE) {
                    //同一个queue可以并行消费
                    //获取消息长度不一致需要，需要修改锁定的长度。
                    block = tryConcurrentBlock(consumer.getTopic(), consumer.getApp(), queueId, consumer.getId(), store, timeout, batchSize, policy);
                    if (block != null) {
                        //由ConcurrentPull控制并发
                        location = new DispatchLocation(this, queue, consumer.getApp());
                        location.getOwner().set(ownerShip);
                    } else {
                        location = null;
                    }
                } else {
                    location = tryQueue(queue, consumer.getApp(), ownerShip);
                }
                if (location != null) {
                    if (location.getQueue().getQueueId() == MessageQueue.RETRY_QUEUE) {
                        // 重试数据，客户端做单条消费消费
                        result = getRetryMessage(consumer, location, (short) 1);
                    } else {

                        // 存储数据
                        if (block != null) {
                            msgBatchSize = concurrentPull.getMessageCount(block);
                            msgQueueOffset = block.getMinQueueOffset();
                        } else {
                            msgQueueOffset = offset;
                            msgBatchSize = batchSize;
                        }
                        result = getStoreMessage(consumer, location, msgBatchSize, msgQueueOffset, delay);
                    }
                    if (result == null || result.getLength() == 0) {
                        if (result != null) {
                            result.release();
                        }
                        // 没有数据，则立即释放占用，尝试下一个队列
                        if (location.getQueue().getQueueId() == MessageQueue.RETRY_QUEUE) {
                            // 重试队列需要重新开始技术
                            synchronized (location) {
                                if (location.getOwner().compareAndSet(ownerShip, null)) {
                                    location.resetOffset();
                                }
                            }
                        } else {
                            location.getOwner().compareAndSet(ownerShip, null);
                            if (block != null) {
                                long expireTime = SystemClock.now() + DEFAULT_BLOCK_EXPIRED;
                                if (result != null) {
                                    MinTimeOffset timeOffset = result.getTimeOffset();
                                    if (timeOffset != null) {
                                        expireTime = timeOffset.getTimeStamp() + delay;
                                    }
                                }
                                releaseBlock(block, consumer, queueId, expireTime);
                            }
                        }
                    } else if (location
                            .updatePullOffset(ownerShip, result.getMinQueueOffset(), result.getMaxQueueOffset(),
                                    result.getNextOffset())) {
                        // 取到数据，连接没有断开，设置过期时间
                        long now = SystemClock.now();
                        ownerShip.setExpireTime(now + timeout);
                        ownerShip.setCreateTime(now);
                        if (block != null && result != null && (result.getMinQueueOffset() != block.getMinQueueOffset() || result.getMaxQueueOffset() != block.getMaxQueueOffset())) {
                            resetBlock(result, block, consumer, queueId, timeout);
                        } else if (block != null) {
                            block.setDispatched(true);
                        }
                        break;
                    } else {
                        // 连接断开了，不用返回数据
                        result = null;
                        break;
                    }
                }
            }
            if (result != null) {
                if (queueId != MessageQueue.RETRY_QUEUE){
                    autoAckForNoJournal(consumer.getTopic(), queueId, result.getAckOffset());
                }
                return new PullResult(consumer.getTopic(), consumer.getApp(), (short)queueId, result.getBuffers(),
                        ownerShip,
                        this);
            }
            return null;
        } catch (Throwable e) {
            // 释放队列所有权
            if (location != null) {
                location.release(ownerShip);
            }
            // 释放资源
            if (result != null) {
                result.release();
            }
            if (e instanceof JMQException) {
                throw (JMQException) e;
            }
            throw new JMQException(e, JMQCode.CN_UNKNOWN_ERROR.getCode());
        }
    }

    /**
     * 确认掉无效的消费位置
     *
     * @param topic
     * @param queueId
     * @param ackOffset
     */
    private void autoAckForNoJournal(String topic, int queueId, long ackOffset) {
        TopicOffset topicOffset = offsetManager.getOffset(topic);
        if (topicOffset != null) {
            QueueOffset consumerOffset = topicOffset.getOffset(queueId);
            for (Map.Entry<String, UnSequenceOffset> e : consumerOffset.getOffsets().entrySet()) {
                long currentAckOffset = e.getValue().getAckOffset().get();
                while (currentAckOffset < ackOffset) {
                    currentAckOffset += ConsumeQueue.CQ_RECORD_SIZE;
                    offsetManager.acknowledge(e.getKey(), new MessageLocation(topic, (short)queueId, currentAckOffset));
                }
            }
        }
    }

    private void resetBlock(GetResult result, OffsetBlock block, Consumer consumer, int queueId, long timeout) {

        List<OffsetBlock> blocks = new ArrayList<OffsetBlock>();
        //对比锁定的块和获取消息的长度是否一致，不一致的要按实际大小进行分裂
        if (result.getMinQueueOffset() > block.getMinQueueOffset()) {
            OffsetBlock partitionMsgBlock = new OffsetBlock(consumer.getId(), queueId, block.getMinQueueOffset(), result.getMinQueueOffset() - ConsumeQueue.CQ_RECORD_SIZE);
            partitionMsgBlock.setExpireTime(SystemClock.now() + DEFAULT_BLOCK_EXPIRED);//立即过期
            blocks.add(partitionMsgBlock);
        }

        if (result.getMaxQueueOffset() < block.getMaxQueueOffset()) {
            OffsetBlock partitionMsgBlock = new OffsetBlock(consumer.getId(), queueId, result.getMaxQueueOffset() + ConsumeQueue.CQ_RECORD_SIZE, block.getMaxQueueOffset());
            partitionMsgBlock.setExpireTime(SystemClock.now() + DEFAULT_BLOCK_EXPIRED);//立即过期
            blocks.add(partitionMsgBlock);
        }

        OffsetBlock lastBlock = new OffsetBlock(consumer.getId(), queueId, result.getMinQueueOffset(), result.getMaxQueueOffset());
        lastBlock.setExpireTime(SystemClock.now() + timeout);
        lastBlock.setDispatched(true);

        ConcurrentPull.PullStat pullStat = concurrentPull.getOrCreatePullStat(consumer.getTopic(), consumer.getApp(), queueId);
        pullStat.splitMessageBlock(block, lastBlock, blocks);
        if (logger.isDebugEnabled()) {
            logger.debug("split block,old" + block + ";new:" + block + ";nodispatch:" + blocks.toString());
        }
    }

    /**
     * 释放掉block的占用
     *
     * @param block
     * @param consumer
     * @param queueId
     * @param expireTime 过期时间点
     */
    private void releaseBlock(OffsetBlock block, Consumer consumer, int queueId, long expireTime) {
        ConcurrentPull.PullStat pullStat = concurrentPull.getOrCreatePullStat(consumer.getTopic(), consumer.getApp(), queueId);
        pullStat.releaseBlock(block, expireTime);
        if (logger.isDebugEnabled()) {
            logger.debug("release block :" + block);
        }
    }

    /**
     * 获取派发队列，并排序
     *
     * @param consumer       消费者
     * @param consumerPolicy 消费策略
     * @param topic          主题
     * @return 派发队列列表
     */
    protected List<DispatchQueue> getSortedQueues(Consumer consumer, TopicConfig.ConsumerPolicy consumerPolicy, DispatchTopic topic) {
        // 构造队列列表
        List<DispatchQueue> queues = new ArrayList<DispatchQueue>();
        if (consumer == null || topic == null) {
            return queues;
        }

        // 普通队列的起始位置
        int beginIndex = -1;
        List<DispatchQueue> normalQueues;
        if (consumerPolicy != null && consumerPolicy.checkSequential()) {
            //顺序消费
            normalQueues = topic.getNormalQueuesWithSequential(consumer);
        } else {
            normalQueues = topic.getNormalQueues();
        }
        AtomicLong counter = getAndCreateCounter(consumer);
        if (!normalQueues.isEmpty()) {
            beginIndex = (int) (counter.incrementAndGet() % normalQueues.size());
        }
        // 高优先级
        queues.add(topic.getQueue(MessageQueue.HIGH_PRIORITY_QUEUE));
        Boolean seeTryQueue = Boolean.FALSE;
        if (consumer.getType().equals(Consumer.ConsumeType.JMQ)) {
            // 判断重试
            seeTryQueue = Boolean.TRUE;
            if ((consumerPolicy != null && (consumerPolicy.checkSequential() || !consumerPolicy.isRetry()))
                    || (counter != null && config.getRetryConfig().getRetryLimitSpeed() > 1 && (counter.get() % config.getRetryConfig().getRetryLimitSpeed() != 0))) {
                seeTryQueue = Boolean.FALSE;
            }
        }

        if (seeTryQueue) {
            Random r = new Random();
            int rand = r.nextInt(10);
            if (rand >= 8) {
                // 20%的几率优先处理重试队列
                queues.add(topic.getQueue(MessageQueue.RETRY_QUEUE));
                resortQueues(normalQueues, queues, beginIndex);
            } else {
                resortQueues(normalQueues, queues, beginIndex);
                queues.add(topic.getQueue(MessageQueue.RETRY_QUEUE));
            }
        } else {
            resortQueues(normalQueues, queues, beginIndex);
        }

        return queues;
    }

    /**
     * 以index为基准重排序from到to
     *
     * @param from
     * @param to
     * @param index
     */
    private void resortQueues(List<DispatchQueue> from, List<DispatchQueue> to, int index) {

        if (index >= 0) {
            for (int i = index; i < from.size(); i++) {
                to.add(from.get(i));
            }

            for (int i = 0; i < index; i++) {
                to.add(from.get(i));
            }
        }
    }

    /**
     * 获取计数器
     *
     * @param consumer 消费者
     * @return 计数器
     */
    protected AtomicLong getAndCreateCounter(Consumer consumer) {
        if (consumer == null) {
            return null;
        }
        AtomicLong counter = counters.get(consumer);
        if (counter == null) {
            counter = new AtomicLong();
            AtomicLong old = counters.putIfAbsent(consumer, counter);
            if (old != null) {
                counter = old;
            }
        }
        return counter;
    }

    /**
     * 获取存储的消息
     *
     * @param consumer 消费者
     * @param location 位置
     * @param count    数量
     * @param offset   偏移量
     * @return 储的消息列表
     * @throws com.ipd.jmq.common.exception.JMQException
     */
    protected GetResult getStoreMessage(Consumer consumer, DispatchLocation location, int count, long offset, int
            delay) throws JMQException {
        DispatchQueue queue = location.getQueue();
        GetResult getResult = null;
        try {
            boolean needDelay = false;
            if (offset < 0) {//如果给定了offset，则以客户端给的为准
                offset = this.getOffset(queue.getTopic(), queue.getQueueId(), location.getApp());

                if (delay > 0) {
                    needDelay = true;
                    //如果设置了延迟消费，判断队列里最小时间戳+延迟时间是否小于当前时间
                    MinTimeOffset timeOffset = location.getMinTimeOffset();
                    if (timeOffset != null && timeOffset.getTimeStamp() > 0 && offset >= timeOffset.getOffset()) {
                        if ((timeOffset.getTimeStamp() + delay) > SystemClock.now()) {
                            return getResult;
                        }
                    }
                }
            }
            getResult = store.getMessage(queue.getTopic(), queue.getQueueId(), offset, count,
                    NettyDecoder.FRAME_MAX_SIZE - 4, delay);

            if (needDelay && getResult != null) {
                MinTimeOffset timeOffset = getResult.getTimeOffset();
                if (timeOffset != null) {
                    location.setMinTimeOffset(timeOffset);
                }
            }

            if (getResult != null && getResult.getCode() != JMQCode.SUCCESS) {
                throw new JMQException(getResult.getCode());
            }
            return getResult;
        } catch (Throwable e) {
            if (getResult != null) {
                getResult.release();
            }
            if (e instanceof JMQException) {
                throw (JMQException) e;
            }
            throw new JMQException(e, JMQCode.CN_UNKNOWN_ERROR.getCode());
        }
    }

    /**
     * 获取单条重试数据
     *
     * @param consumer 消费者
     * @param location 消费位置
     * @param count    数量
     * @return 重试数据
     * @throws JMQException
     */
    protected GetResult getRetryMessage(Consumer consumer, DispatchLocation location, short count) throws JMQException {
        GetResult getResult = new GetResult();
        getResult.setCode(JMQCode.SUCCESS);
        // 按ID升序排序
        List<BrokerMessage> messages = retryManager.getRetry(consumer.getTopic(), consumer.getApp(), count, location.getPullNext());
        if (messages == null || messages.isEmpty()) {
            // 没有数据了，则从0开始
            getResult.setMinQueueOffset(-1);
            getResult.setMaxQueueOffset(-1);
            getResult.setNextOffset(0);
            getResult.setLength(0);
            return getResult;
        }

        // 取到最大的ID
        BrokerMessage start = messages.get(0);
        BrokerMessage end = messages.get(messages.size() - 1);

        List<RByteBuffer> buffers = new ArrayList<RByteBuffer>();
        getResult.setBuffers(buffers);

        try {
            for (BrokerMessage message : messages) {
                ByteBuf byteBuf = UnpooledByteBufAllocator.DEFAULT.buffer();
                Serializer.write(message, byteBuf);
                buffers.add(new RByteBuffer(byteBuf.nioBuffer(), null));
            }

            getResult.setMinQueueOffset(start.getQueueOffset());
            getResult.setMaxQueueOffset(end.getQueueOffset());
            getResult.setNextOffset(messages.size() < count ? 0 : end.getQueueOffset() + 1);//database id is sequence
            getResult.setLength(buffers.size());
            return getResult;
        } catch (Exception e) {
            throw new JMQException(JMQCode.FW_GET_MESSAGE_ERROR, e);
        }
    }

    /**
     * 尝试选择消费队列。队列没用被占用，有未消费数据，并且没用未应答数据则返回改队列
     *
     * @param messageQueue 消费队列
     * @param app          消费者
     * @param ownerShip    所有者
     * @return 消费位置
     */
    protected DispatchLocation tryQueue(final DispatchQueue messageQueue, final String app, final OwnerShip ownerShip) {
        if (messageQueue == null || app == null || ownerShip == null) {
            return null;
        }
        DispatchLocation location = messageQueue.getLocation(app);
        if (messageQueue.getQueueId() == MessageQueue.RETRY_QUEUE) {
            location.getOwner().set(ownerShip);
            return location;
        } else {
            if (location.isFree()) {
                if (location.getOwner().compareAndSet(null, ownerShip)) {
                    // 提前占用，不要清理
                    return location;
                }
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("location has not any data or is busy:" + location);
        }
        return null;
    }

    private OffsetBlock tryConcurrentBlock(String topic, String app, int queueId, String consumerId, Store store,
                                           long timeout, int count, TopicConfig.ConsumerPolicy policy) throws
            Exception {
        ConcurrentPull.PullStat pullStat = concurrentPull.getOrCreatePullStat(topic, app, queueId);
        int prefetchSize = policy.getPrefetchSize() == null ? config.getPrefetchSize() : (policy.getPrefetchSize().intValue() > 0 ? policy.getPrefetchSize() : config.getPrefetchSize());
        OffsetBlock block = concurrentPull.getConsumeMessageBlock(consumerId, pullStat, store, timeout, count, prefetchSize);
        return block;
    }

    @Override
    public boolean hasFreeQueue(final Consumer consumer) {
        if (consumer == null) {
            return false;
        }
        DispatchTopic topic = getAndCreateConsumeTopic(consumer.getTopic());
        DispatchLocation location;
        for (DispatchQueue messageQueue : topic.getQueues()) {
            location = messageQueue.getLocation(consumer.getApp());
            if (location.isFree()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean acknowledge(final MessageLocation[] locations, final Consumer consumer, final boolean isSuccessAck) throws JMQException {
        boolean flag = false;
        if (consumer == null) {
            throw new JMQException(JMQCode.FW_CONSUMER_NOT_EXISTS);
        }
        if (!isStarted()) {
            throw new JMQException(JMQCode.CN_SERVICE_NOT_AVAILABLE);
        }
        if (locations == null || locations.length == 0) {
            return flag;
        }

        Map<Short, List<MessageLocation>> locationMap = new HashMap<Short, List<MessageLocation>>();
        for (MessageLocation location : locations) {
            if (!location.getTopic().equals(consumer.getTopic())) {
                String str = new StringBuffer().append(location.getTopic()).append(" NotEq ").append(consumer.getTopic())
                        .append(" app:").append(consumer.getApp()).toString();
                logger.error(str);
                throw new JMQException(str, JMQCode.CN_UNKNOWN_ERROR.getCode());
            }

            List<MessageLocation> locationList = locationMap.get(location.getQueueId());
            if (locationList == null) {
                locationList = new ArrayList<MessageLocation>();
                locationMap.put(location.getQueueId(), locationList);
            }
            locationList.add(new MessageLocation(location.getTopic(), location.getQueueId(), location.getQueueOffset(), location.getJournalOffset()));
        }

        for (Map.Entry<Short, List<MessageLocation>> entry : locationMap.entrySet()) {
            List<MessageLocation> list = entry.getValue();
            Collections.sort(list, new Comparator<MessageLocation>() {
                @Override
                public int compare(MessageLocation o1, MessageLocation o2) {
                    if (o1.getQueueOffset() < o2.getQueueOffset()) {
                        return -1;
                    } else if (o1.getQueueOffset() == o2.getQueueOffset()) {
                        return 0;
                    } else {
                        return 1;
                    }
                }
            });
        }
        TopicConfig.ConsumerPolicy policy = clusterManager.checkReadable(consumer.getTopic(), consumer.getApp());
        if (clusterManager.getTopicConfig(consumer.getTopic()).getType() == TopicConfig.TopicType.TOPIC &&
                policy != null && policy.getConcurrentConsume() != null && policy.getConcurrentConsume().booleanValue()) {
            for (Map.Entry<Short, List<MessageLocation>> entry : locationMap.entrySet()) {
                short queueId = entry.getKey();
                if (queueId != MessageQueue.RETRY_QUEUE) {
                    flag = concurrentPull.ack(consumer.getTopic(), consumer.getApp(), consumer.getId(), entry.getValue(), store);
                }
            }
            return flag;
        }
        // 设置应答位置

        DispatchTopic topic = getAndCreateConsumeTopic(consumer.getTopic());
        /*for (Map.Entry<Short, Long> entry : offsets.entrySet()) {
            queueId = entry.getKey();
            queueOffset = entry.getValue();
            queue = topic.getQueue(queueId);
            if (queue == null) {
                continue;
            }
            location = queue.getLocation(consumer.getApp());
            ownerShip = location.getOwner().get();
            // 队列所有权被更换了
            if (ownerShip == null || !consumer.getId().equals(ownerShip.getOwner())) {
                continue;
            }
            // 判断是否是重试队列
            if (queue.getQueueId() == MessageQueue.RETRY_QUEUE) {
                // 重试队列，直接设置应答位置
                location.updateAckOffset(ownerShip, queueOffset);
                // 释放队列所有权
                location.getOwner().compareAndSet(ownerShip, null);
                flag = true;
            } else if (location.getAckOffset() < queueOffset && (location
                    .getPullEnd() == queueOffset || queueOffset == store
                    .getOffset(location.getTopic(), queueId, consumer.getApp()))) {
                // 上次应答应答位置小于当前应答位置，是最后一条或者连续的应答位置
                if (logger.isDebugEnabled()) {
                    logger.debug(
                            String.format("ack queueId:%d,queueOffset:%d,ackOffset:%d,pullEnd:%d", queueId, queueOffset,
                                    location.getAckOffset(), location.getPullEnd()));
                }
                location.updateAckOffset(ownerShip, queueOffset);
                // 全部应答完
                store.acknowledge(new MessageLocation[]{new MessageLocation(location.getTopic(), queueId, queueOffset)},
                        consumer.getApp());
                // 判断是否还有未应答数据
                if (!location.hasNoAck()) {
                    // 释放占用
                    location.getOwner().compareAndSet(ownerShip, null);
                }
                flag = true;
            }
        }*/
        for (Map.Entry<Short, List<MessageLocation>> entry : locationMap.entrySet()) {
            short queueId = entry.getKey();
            DispatchQueue queue = topic.getQueue(queueId);
            if (queue == null) {
                logger.error("queue " + queueId + " is null" + consumer.getTopic());
                continue;
            }

            DispatchLocation dispatchLocation = queue.getLocation(consumer.getApp());
            OwnerShip ownerShip = dispatchLocation.getOwner().get();
            // 队列所有权被更换了
            if (ownerShip == null || !consumer.getId().equals(ownerShip.getOwner())) {
                continue;
            } else {
                if (isSuccessAck) {
                    //如果是报错确认的就不清理
                    cleanConsumeErr(consumer.getId());
                }
            }

            List<MessageLocation> locationList = entry.getValue();
            for (MessageLocation ackLoc : locationList) {
                long nextAckOffset = this.getOffset(ackLoc.getTopic(), queueId, consumer.getApp());
                // 判断是否是重试队列
                if (queue.getQueueId() == MessageQueue.RETRY_QUEUE) {
                    // 重试队列，直接设置应答位置
                    dispatchLocation.updateAckOffset(ownerShip, ackLoc.getQueueOffset());
                    // 释放队列所有权
                    dispatchLocation.getOwner().compareAndSet(ownerShip, null);
                    flag = true;
                } else if (ackLoc.getQueueOffset() == nextAckOffset) {
                    //连续的应答位置
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                                String.format("ack queueId:%d,queueOffset:%d,ackOffset:%d,pullEnd:%d", queueId, ackLoc.getQueueOffset(),
                                        dispatchLocation.getAckOffset(), dispatchLocation.getPullEnd()));
                    }
                    dispatchLocation.updateAckOffset(ownerShip, ackLoc.getQueueOffset());
                    // 全部应答完
                    this.acknowledge(new MessageLocation(ackLoc.getTopic(), queueId, ackLoc.getQueueOffset()), consumer
                            .getApp());
                    flag = true;
                } else {
                    logger.error(String.format("no continues cur:%d,next:%d, topic:%s,app:%s,queueId:%d ", ackLoc.getQueueOffset(), nextAckOffset, consumer.getTopic(), consumer.getApp(), ackLoc.getQueueId()));
                    break;
                }
            }
            // 判断是否还有未应答数据
            if (!dispatchLocation.hasNoAck()) {
                // 释放占用
                dispatchLocation.getOwner().compareAndSet(ownerShip, null);
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("has not ack " + dispatchLocation);
                }
            }

        }
        return flag;
    }

    @Override
    public boolean cleanExpire(final String topic, final String app, final int queueId) {
        return cleanExpire(topic, app, queueId, null);
    }

    @Override
    public boolean cleanExpire(final String topic, final String app, final int queueId, final OwnerShip ownerShip) {
        // 得到派发主题
        DispatchTopic consumeTopic = topics.get(topic);
        if (consumeTopic == null) {
            return false;
        }

        // 得到派发队列
        DispatchQueue queue = consumeTopic.getQueue(queueId);
        if (queue == null) {
            return false;
        }
        // 得到派发位置
        DispatchLocation location = queue.getLocation(app);
        if (location == null) {
            return false;
        }
        // 按照指定的所有权进行释放
        if (ownerShip != null) {
            return location.release(ownerShip);
        } else {
            // 取到当前所有权
            OwnerShip current = location.getOwner().get();
            if (current != null) {
                // 按照当前所有权进行释放
                return location.release(current);
            } else if (location.hasNoAck()) {
                // 没有被占用，但有未应答数据
                return location.release(SystemClock.getInstance().now());
            }
            // 没有被占用，并且也没有未应答数据
            return true;
        }
    }

    @Override
    public void cleanExpire(final Consumer consumer, final int queueId) {
        if (consumer == null) {
            return;
        }
        cleanExpire(consumer.getTopic(), consumer.getApp(), queueId, null);
    }

    @Override
    public ConcurrentMap<Joint, Boolean> getLeaders() {
        return leaders;
    }

    @Override
    public boolean isPendingExcludeRetry(String topic, String app) {
        return this.getPending(topic, app) > 0;
    }

    @Override
    public boolean addListener(EventListener<DispatchEvent> eventListener) {
        return dispatchEventManager.addListener(eventListener);
    }


    public Map<ConcurrentPull.PullStatKey, List<OffsetBlock>> getNoAckBlocks() {
        return concurrentPull.getNoAckBlocks();
    }

    @Override
    public void resetAckOffset(String topic, int queueId, String consumer, long offset) throws JMQException {
        checkState();

        if (0 != (offset % ConsumeQueue.CQ_RECORD_SIZE)) {
            offset -= (offset % ConsumeQueue.CQ_RECORD_SIZE);
        }

        long minOffset = store.getMinOffset(topic, queueId);
        long maxOffset = store.getMaxOffset(topic, queueId);
        if (minOffset > offset || maxOffset < offset) {
            logger.error("Invalid offset! offset=" + offset + ",min=" + minOffset + ", max=" + maxOffset);
            return;
        }


        // 检查offsetValue的有效性， 如果无效则提供足够的日志信息
        //可根据日志信息重新计算offsetValue
        if (offset < maxOffset) {
            //offsetValue == maxoffset时，表示重置到最大消费位置,不做检查
            try {
                //检查索引位置对应的数据文件是否存在
                QueueItem item = store.getQueueItem(topic, queueId, offset);
                if (item == null) {
                    logger.error("Invalid offset, no queue item found!" + offset + "--minoffset--" + minOffset);
                    return;
                }

                if (item.getJournalOffset() < store.getMinOffset()) {
                    logger.error("Invalid Journal--" + item + "--minJournal--" + store.getMinOffset());

                    //查找最小的有效索引
                    long start = offset + ConsumeQueue.CQ_RECORD_SIZE;
                    long end = store.getMaxOffset(topic, queueId) - ConsumeQueue.CQ_RECORD_SIZE;
                    QueueItem minItem = store.getMinQueueItem(topic, queueId, start, end);

                    logger.error("--MinItem in journal is--" + minItem + "--start--" + start + "--end--" + end);
                    return;
                }
            } catch (Exception e) {
                logger.error(String.format("resetAckOffset error, topic:%s,queueId:%d,offset:%d,app:%s,min:%d, max:%d", topic, queueId, offset, consumer, minOffset, maxOffset), e);
                return;
            }
        }


        if (logger.isDebugEnabled()) {
            logger.debug(String.format("reset ack offset topic:%s,queueId:%d,offset:%d,app:%s", topic, queueId, offset, consumer));
        }
        offsetManager.resetAckOffset(topic, queueId, consumer, offset);

    }

    @Override
    public void ackConsumerExpiredMessage(String topic, String consumer) throws JMQException {
        List<Integer> queues = store.getQueues(topic);
        if (queues == null || queues.isEmpty()) {
            return;
        }
        for (int queueId : queues) {
            long nextAckOffset = getOffset(topic, queueId, consumer);
            long offsetValue = store.getQueueOffsetByTimestamp(topic, queueId, consumer, SystemClock.now() - store
                    .getConfig().getBroadcastOffsetAckTimeDifference(), nextAckOffset);
            if (offsetValue < 0) {
                continue;
            }

            if (nextAckOffset > offsetValue) {
                continue;
            }
            logger.info(String.format("ack expired message topic:%s,queueId:%d,offset:%d,app:%s", topic, queueId, offsetValue, consumer));
            resetAckOffset(topic, queueId, consumer, offsetValue);
        }
    }

    @Override
    public UnSequenceOffset getUnSequenceOffset(String topic, String app, int queueId) {
        return offsetManager.getOffset(topic, queueId, app);
    }

    protected void acknowledge(MessageLocation[] locations, String consumer) {
        if (locations == null) {
            return;
        }
        for (MessageLocation location : locations) {
            acknowledge(location, consumer);
        }
    }

    protected void acknowledge(MessageLocation location, String consumer) {
        if (location == null || consumer == null || consumer.isEmpty()) {
            return;
        }

        checkState();
        // 转换为下一次索引开始的位置
        String topic = location.getTopic();
        short queueId = location.getQueueId();
        long offset = store.getNextOffset(topic, queueId, location.getQueueOffset());

        // 需要复制一份数据
        offsetManager.acknowledge(consumer, new MessageLocation(topic, queueId, offset));
    }

    @Override
    public long getOffset(String topic, int queueId, String consumer) {
        if (queueId == MessageQueue.RETRY_QUEUE) {
            return 0;
        }
        long localOffset;
        // 得到消费位置
        Offset offset = offsetManager.getOffset(topic, queueId, consumer);
        // 得到队列的最小位置
        long minOffset = store.getMinOffset(topic, queueId);
        if (offset == null) {
            // 新增了队列，订阅关系还没有维护
            localOffset = minOffset;
        } else {
            localOffset = offset.getAckOffset().get();
            if (localOffset < minOffset) {
                // 消费位置没有同步等原因
                localOffset = minOffset;
            }
        }

        return localOffset;
    }

    @Override
    public Set<Long> getOffsetsForKafkaBefore(String topic, int queueId, long timestamp, int maxNumOffsets) {
        if (queueId == MessageQueue.RETRY_QUEUE) {
            return Collections.singleton(0L);
        }

        if (timestamp == -1L) {
            // 得到队列的最大位置
            return Collections.singleton(store.getMaxOffset(topic, queueId));
        } else if (timestamp == -2L) {
            // 得到队列的最小位置
            return Collections.singleton(store.getMinOffset(topic, queueId));
        } else {
            try {
                return store.getQueueOffsetsBefore(topic, queueId, timestamp, maxNumOffsets);
            } catch (JMQException e) {
                logger.warn("topic={}, queue={} get offset before time={} failed! Will return the latest", topic, queueId, timestamp, e);
                return Collections.singleton(store.getMaxOffset(topic, queueId));
            }
        }
    }

    @Override
    public TopicOffset getOffset(String topic) {
        return offsetManager.getOffset(topic);
    }

    @Override
    public long getPending(String topic, String consumer) {
        // 得到主题所有队列
        List<Integer> queues = store.getQueues(topic);
        // 得到主题消费位置
        TopicOffset topicOffset = offsetManager.getOffset(topic);
        if (queues == null || topicOffset == null) {
            return 0;
        }

        long pending = 0;
        QueueOffset queueOffset;
        UnSequenceOffset offset;
        // 遍历每个队列
        for (Integer queueId : queues) {
            // 得到队列
            // 得到队列的消费位置
            queueOffset = topicOffset.getOffset(queueId);
            if (queueOffset != null) {
                // 得到应用消费该队列的位置
                offset = queueOffset.getOffset(consumer);
                if (offset != null) {
                    long baseAckOffset = offset.getAckOffset().get();
                    //maxoffset 为writePosition ,此处不需要 +1
                    long curPending = (store.getMaxOffset(topic, queueId) - baseAckOffset) / ConsumeQueue.CQ_RECORD_SIZE;

                    List<Sequence> sequenceList = offset.getAcks().allSequence();
                    for (Sequence sequence : sequenceList) {
                        if (sequence.getLast() <= baseAckOffset) {
                            //忽略
                        } else if (sequence.getFirst() <= baseAckOffset && sequence.getLast() > baseAckOffset) {
                            curPending = curPending - ((sequence.getLast() - baseAckOffset) / ConsumeQueue.CQ_RECORD_SIZE + 1);
                        } else {
                            curPending = curPending - sequence.range();
                        }
                    }
                    pending += curPending;
                }
            }
        }

        return pending;
    }

    // 全量订阅
    @Override
    public void subscribe(String topic, String consumer) {
        checkState();
        offsetManager.addConsumer(topic, consumer);
    }

    @Override
    public void unSubscribe(String topic, String consumer) {
        checkState();
        offsetManager.removeConsumer(topic, consumer);
    }

    public BrokerMessage dispatchQueue(final BrokerMessage message) throws Exception {
        // 获取队列数量
        int queueId;
        short count = offsetManager.getQueueCount(message.getTopic());
        if (count <= 0) {
            // 消费队列不存在
            throw new JMQException(JMQCode.SE_QUEUE_NOT_EXISTS, message.getTopic());
        }

        // KAFKA会直接指定高优先级队列ID
        if (message.getQueueId() > 0 && message.getQueueId() <= count || message.getQueueId() == MessageQueue.HIGH_PRIORITY_QUEUE) {
            // 指定了队列号,存储到该队列号,包括严格顺序消息
            queueId = message.getQueueId();
        } else if (message.getPriority() > MessageQueue.DEFAULT_PRIORITY) {
            // 判断是否是高优先级队列
            queueId = MessageQueue.HIGH_PRIORITY_QUEUE;
        } else if (message.isOrdered() && (message.getBusinessId() != null && !message.getBusinessId().isEmpty())) {
            // 非严格顺序消息,未设置期望的queueId,通过业务ID计算queueId
            int hashCode = message.getBusinessId().hashCode();
            hashCode = hashCode > Integer.MIN_VALUE ? hashCode : Integer.MIN_VALUE + 1;
            queueId =  (Math.abs(hashCode) % count + 1);
        } else {
            // 普通消息
            queueId =  ((int) (Math.random() * count) + 1);
        }

        List<Integer> queues = store.getQueues(message.getTopic());
        if (!queues.contains(queueId)) {
            // 消费队列还不存在
            if (message.getQueueId() > MessageQueue.MAX_NORMAL_QUEUE) {
                // 增加优先级队列
                addPriorityQueue(message.getTopic(), message.getQueueId());
            } else {
                // 更新队列数量
                offsetManager.updateQueueCount(message.getTopic(), count);
            }
        }

        message.setQueueId((short) queueId);

        return message;
    }

    private void addPriorityQueue(final String topic, final int queueId) throws JMQException {
        if (!isStarted() || !isReady()) {
            throw new JMQException("store is stopped.", JMQCode.CN_SERVICE_NOT_AVAILABLE.getCode());
        }
        try {
            // 创建队列
            store.addQueue(topic, (short) queueId);
            // 增加消费者
            offsetManager.addConsumer(topic, store.getQueues(topic));
        } catch (JMQException e) {
            throw e;
        } catch (Exception e) {
            throw new JMQException(e.getMessage(), e, JMQCode.SE_IO_ERROR.getCode());
        }
    }

    protected void checkState() {
        if (!isStarted() || !isReady()) {
            throw new IllegalStateException(JMQCode.CN_SERVICE_NOT_AVAILABLE.getMessage());
        }
    }


    /**
     * 清理应答过期的数据
     */
    protected void cleanExpire() {
        long now = SystemClock.now();
        ConcurrentMap<String, ConcurrentMap<String, AtomicInteger>> expireStat = new ConcurrentHashMap<String,
                ConcurrentMap<String, AtomicInteger>>();
        for (Map.Entry<String, DispatchTopic> entry : topics.entrySet()) {
            DispatchTopic dispatchTopic = entry.getValue();
            dispatchTopic.cleanExpire(this, now, expireStat);
        }
        //统计消费超时次数
        if (brokerMonitor != null && !expireStat.isEmpty()) {
            brokerMonitor.onCleanExpire(expireStat);
        }
        expireStat.clear();
        cleanErrStats(now);
    }

    /**
     * 清理断开了但未清理掉的
     */
    protected void sinkSequentialQueues() {
        try {
            ConcurrentMap<String, DispatchTopic> topics = this.topics;
            if (topics != null && !topics.isEmpty()) {
                for (DispatchTopic topic : topics.values()) {
                    ConcurrentMap<String, Map<String, List<Integer>>> seqConsumers = topic.getSequentialConsumer();
                    if (seqConsumers != null && !seqConsumers.isEmpty()) {
                        for (Map<String, List<Integer>> consumers : seqConsumers.values()) {
                            if (consumers != null) {
                                Iterator<String> it = consumers.keySet().iterator();
                                if (it.hasNext()) {
                                    String consumerId = it.next();
                                    if (sessionManager.getConsumerById(consumerId) == null) {
                                        consumers.remove(consumerId);
                                    }
                                }
                            }
                        }
                    }

                }
            }
        } catch (Exception e) {
            logger.error("clean dead consumer error!", e);
        }
    }


    private void cleanErrStats(long now) {
        //如果客户端长时间被锁，这里可能就已经清理了，因此这里就不再处理
        try {
            List<String> clearConsumerIds = new ArrayList<String>();
            for (Map.Entry<String, ConsumerErrStat> errStatEntry : errStats.entrySet()) {
                ConsumerErrStat stat = errStatEntry.getValue();
                if (stat != null) {
                    if (stat.getTimestamp() + 60 * 60 * 1000 < now) {
                        logger.info("clean expired consumer errstat:" + String.format("%d + %d < %d", stat.getTimestamp(), 10 * 60 * 1000, now));
                        clearConsumerIds.add(stat.getConsumerId());
                    }
                }
            }

            for (String consumerId : clearConsumerIds) {
                errStats.remove(consumerId);
            }
        } catch (Throwable e) {
            logger.error("", e);
        }
    }

    @Override
    public List<String> getLocations(String topic, String app) {
        List<String> locations = new ArrayList<String>();
        for (Map.Entry<String, DispatchTopic> entry : topics.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(topic.trim())) {
                List<DispatchQueue> queues = entry.getValue().getQueues();
                for (DispatchQueue q : queues) {
                    DispatchLocation location = q.getLocation(app);
                    locations.add(location.toString());
                }
            }
        }
        return locations;
    }

    protected void onAddConsumer(final Consumer consumer) throws JMQException {
        this.subscribe(consumer.getTopic(), consumer.getApp());
        // 初始化消费者队列和偏移量数据
        DispatchTopic consumeTopic = getAndCreateConsumeTopic(consumer.getTopic());
        for (DispatchQueue queue : consumeTopic.getQueues()) {
            queue.getLocation(consumer.getApp());
        }
    }

    protected void onRemoveConsumer(final Consumer consumer) throws JMQException {
        //移除计数引用
        AtomicLong count = counters.remove(consumer);
        DispatchTopic consumeTopic = topics.get(consumer.getTopic());
        //移除顺序消息绑定的queue
        consumeTopic.removeBindConsumer(consumer);

        // 遍历队列
        for (DispatchQueue queue : consumeTopic.getQueues()) {
            // 取到消费位置
            DispatchLocation location = queue.getLocation(consumer.getApp());
            if (location != null) {
                // 判断所有者是否是待移除的消费者
                OwnerShip ownerShip = location.getOwner().get();
                if (ownerShip != null && consumer.getId().equals(ownerShip.getOwner())) {
                    location.release(ownerShip);
                }
            }
        }
        errStats.remove(consumer.getId());
    }

    protected void onAddProducer(final Producer producer) throws JMQException {
        short queues = 0;
        TopicConfig config = clusterManager.getTopicConfig(producer.getTopic());
        if (config != null) {
            queues = config.checkSequential() ? 1 : config.getQueues();
        }
        store.updateQueues(producer.getTopic(), queues);
    }

    /**
     * 从存储恢复所有队列
     */
    protected void restoreQueues() {
        // 从存储获取所有队列
        ConcurrentMap<String, List<Integer>> messageQueues = store.getQueues();
        DispatchTopic topic;
        DispatchQueue queue;
        // 遍历主题
        for (Map.Entry<String, List<Integer>> entry : messageQueues.entrySet()) {
            // 获取消费主题，设置队列数量
            topic = getAndCreateConsumeTopic(entry.getKey());
            // 遍历队列
            for (Integer queueId : entry.getValue()) {
                // 过滤掉重试队列
                if (queueId != MessageQueue.RETRY_QUEUE) {
                    if (queueId != MessageQueue.HIGH_PRIORITY_QUEUE) {
                        // 设置普通队列数量
                        if (topic.getCount() < queueId) {
                            topic.setCount(queueId);
                        }
                    }
                    // 设置偏移量
                    queue = topic.getQueue(queueId);
                    queue.setMaxOffset(store.getMaxOffset(entry.getKey(), queueId));
                    //queue.setMinOffset(queueEntry.getValue().getMinOffset());//QUEUE会删除，可能不对
                }
            }
        }
    }

    /**
     * 获取主题对象
     *
     * @param topic 主题
     * @return 主题对象
     */
    protected DispatchTopic getAndCreateConsumeTopic(final String topic) {
        DispatchTopic consumeTopic = topics.get(topic);
        if (consumeTopic == null) {
            consumeTopic = new DispatchTopic(store, this, topic);
            DispatchTopic old = topics.putIfAbsent(topic, consumeTopic);
            if (old != null) {
                consumeTopic = old;
            }
        }
        return consumeTopic;
    }

    /**
     * 队列数发生变更
     *
     * @param topic  队列
     * @param queues 数量
     * @throws JMQException
     */
    protected void onQueueChange(final String topic, final short queues) throws JMQException {
        DispatchTopic consumeTopic = getAndCreateConsumeTopic(topic);
        if (consumeTopic.getCount() != queues) {
            consumeTopic.setCount(queues);
        }
    }

    @Override
    public void onEvent(final LeaderEvent event) {
        List<String> nodes = Path.nodes(event.getPath());
        if (nodes == null || nodes.isEmpty()) {
            return;
        }
        int size = nodes.size();
        if (size < 2) {
            return;
        }
        // 重试Leader选举
        String topic = nodes.get(size - 2);
        String app = nodes.get(size - 1);
        Joint joint = new Joint(topic, app);
        switch (event.getType()) {
            case TAKE:
                leaders.put(joint, Boolean.TRUE);
                break;
            case LOST:
                leaders.remove(joint);
                break;
        }
    }

    public EventListener getTopicListener() {
        return topicListener;
    }

    /**
     * 队列状态
     */
    enum QueueState {
        /**
         * 删除了
         */
        DELETED,
        /**
         * 目前没用数据了，稍后删除
         */
        DELETING,
        /**
         * 准备删除，可以继续消费剩余的数据
         */
        PREPARE_DELETE,
        /**
         * 使用中
         */
        USED,
    }


    /**
     * 消费主题
     */
    static class DispatchTopic {
        // 存储
        private Store store;
        // 派发服务
        private DispatchService service;
        // 名称
        private String name;
        // 普通优先级队列数
        private int count;
        // 重试队列
        private DispatchQueue retryQueue;
        // 高优先级队列
        private DispatchQueue highQueue;
        // 普通优先级队列
        private List<DispatchQueue> normalQueues = new CopyOnWriteArrayList<DispatchQueue>();
        // 顺序消息的消费者和队列的绑定关系
        private ConcurrentMap<String, Map<String, List<Integer>>> sequentialConsumer = new ConcurrentHashMap<String,
                Map<String, List<Integer>>>();
        // 锁
        private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

        public DispatchTopic(Store store, DispatchService service, String name) {
            if (store == null) {
                throw new IllegalArgumentException("store can not be null");
            }
            if (service == null) {
                throw new IllegalArgumentException("dispatch service can not be null");
            }
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("name can not be empty");
            }
            this.store = store;
            this.service = service;
            this.name = name;
            this.retryQueue = new DispatchQueue(store,service, name, MessageQueue.RETRY_QUEUE);
            this.highQueue = new DispatchQueue(store, service,name, MessageQueue.HIGH_PRIORITY_QUEUE);
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            if (count <= 0 || count > MessageQueue.MAX_NORMAL_QUEUE) {
                throw new IllegalArgumentException("count must be between 1 and " + MessageQueue.MAX_NORMAL_QUEUE);
            }
            DispatchQueue dispatchQueue;
            lock.writeLock().lock();
            try {
                int cnt = normalQueues.size();
                int size = count - cnt;
                if (size > 0) {
                    // 原来的队列改成使用状态
                    for (DispatchQueue queue : normalQueues) {
                        queue.getState().set(QueueState.USED);
                    }
                    // 新增队列，默认是使用状态
                    for (int i = 0; i < size; i++) {
                        normalQueues.add(new DispatchQueue(store,service, name, (short) (cnt + i + 1)));
                    }
                } else if (size < 0) {
                    // 删除多余的队列，只是把状态改成准备删除
                    for (int i = 0; i < Math.abs(size); i++) {
                        dispatchQueue = normalQueues.get(cnt - i - 1);
                        dispatchQueue.getState().compareAndSet(QueueState.USED, QueueState.PREPARE_DELETE);
                    }
                }
                this.count = count;
            } finally {
                lock.writeLock().unlock();
            }

        }

        /**
         * 返回所有队列，包括重试队列，高优先级队列，普通队列
         *
         * @return 队列列表
         */
        public List<DispatchQueue> getQueues() {
            lock.readLock().lock();
            try {
                List<DispatchQueue> queues = new ArrayList<DispatchQueue>(normalQueues.size() + 2);
                queues.add(retryQueue);
                queues.add(highQueue);
                queues.addAll(normalQueues);
                return queues;
            } finally {
                lock.readLock().unlock();
            }
        }

        /**
         * 判断是否积压,包括高优先级队列和普通队列
         *
         * @param app
         * @return
         */
        public boolean isBacklogExcludeRetry(String app) {
            if (highQueue == null || !highQueue.isBacklog(app)) {
                if (!normalQueues.isEmpty()) {
                    for (DispatchQueue queue : normalQueues) {
                        if (queue.isBacklog(app)) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        /**
         * 获取普通队列
         *
         * @return 普通队列列表
         */
        public List<DispatchQueue> getNormalQueues() {
            lock.readLock().lock();
            try {
                List<DispatchQueue> queues = new ArrayList<DispatchQueue>(normalQueues.size());
                queues.addAll(normalQueues);
                return queues;
            } finally {
                lock.readLock().unlock();
            }
        }

        public void removeBindConsumer(Consumer consumer) {
            if (consumer == null) {
                return;
            }
            Map<String, List<Integer>> consumers = sequentialConsumer.get(consumer.getApp());
            if (consumers != null) {
                consumers.remove(consumer.getId());
            }
        }

        /**
         * 顺序消费获取普通队列
         *
         * @param consumer 消费者
         * @return 普通队列列表
         */
        public List<DispatchQueue> getNormalQueuesWithSequential(Consumer consumer) {
            ConcurrentMap<String, Map<String, List<Integer>>> sequentialConsumer = this.sequentialConsumer;

            int queueSize = normalQueues.size();
            List<DispatchQueue> queues = new ArrayList<DispatchQueue>(queueSize);
            //consumers:Map<consumerId,List<queueId>>
            Map<String, List<Integer>> consumers = sequentialConsumer.get(consumer.getApp());

            if (consumers == null) {
                consumers = new LinkedHashMap<String, List<Integer>>(queueSize + 1);
                Map<String, List<Integer>> old = sequentialConsumer.putIfAbsent(consumer.getApp(), consumers);
                if (old != null) {
                    consumers = old;
                }
            }

            String consumerId = consumer.getId();
            List<Integer> assignedQueues = consumers.get(consumerId);

            int assignedQueueCount = 0;
            for (List<Integer> temp : consumers.values()) {
                assignedQueueCount += temp.size();
            }

            //1.消费者数量减少了
            if (assignedQueueCount < queueSize ||
                    (consumers.size() < queueSize) && (assignedQueues == null || assignedQueues.isEmpty())) {

                lock.writeLock().lock();
                try {
                    if (assignedQueueCount < queueSize) {
                        assignedQueueCount = 0;
                        for (List<Integer> temp : consumers.values()) {
                            assignedQueueCount += temp.size();
                        }
                    }

                    assignedQueues = consumers.get(consumerId);
                    int consumerSize = consumers.size();
                    if (assignedQueueCount < queueSize || (consumerSize < queueSize) && (assignedQueues == null || assignedQueues.isEmpty())) {

                        List<Integer> newAssignedQueues = new ArrayList<Integer>();
                        // 将所有队列分配给，第一个消费者
                        if (consumerSize == 0) {
                            for (DispatchQueue queue : normalQueues) {
                                newAssignedQueues.add(queue.getQueueId());
                            }
                            consumers.put(consumerId, newAssignedQueues);
                        } else {
                            int avg = queueSize / consumerSize;
                            int mod = queueSize % consumerSize;

                            if (assignedQueueCount < queueSize) {
                                if (consumerSize < queueSize) {
                                    consumers.put(consumerId, newAssignedQueues);
                                }
                                //有消费者数量减少，重新分配
                                int i = 1;
                                for (String cid : consumers.keySet()) {
                                    int j = 1;
                                    List<Integer> newList = new ArrayList<Integer>();
                                    int count = j * avg;
                                    if (--mod >= 0) {
                                        count += 1;
                                    }

                                    for (; i <= count; i++) {
                                        newList.add(i);
                                    }
                                    consumers.put(cid, newList);
                                    j++;
                                }
                            } else {
                                // 消费者数量增加
                                for (List<Integer> oldQueues : consumers.values()) {
                                    int exceed = oldQueues.size() - avg;
                                    if (--mod >= 0) {
                                        exceed -= 1;
                                    }
                                    if (exceed > 0) {
                                        for (int i = 0; i < queueSize && exceed >= 0; i++, exceed--) {
                                            newAssignedQueues.add(oldQueues.remove(avg + exceed));
                                        }
                                    }
                                }
                                consumers.put(consumerId, newAssignedQueues);
                            }
                        }

                        assignedQueues = consumers.get(consumerId);
                    }
                } finally {
                    lock.writeLock().unlock();
                }
            }

            if (assignedQueues != null && !assignedQueues.isEmpty()) {
                for (DispatchQueue queue : normalQueues) {
                    if (assignedQueues.contains(queue.getQueueId())) {
                        queues.add(queue);
                    }
                }
            }

            return queues;
        }

        /**
         * 根据队列ID返回队列
         *
         * @param queueId 队列ID
         * @return 队列
         */
        public DispatchQueue getQueue(int queueId) {
            if (queueId == MessageQueue.RETRY_QUEUE) {
                return retryQueue;
            }
            if (queueId == MessageQueue.HIGH_PRIORITY_QUEUE) {
                return highQueue;
            }
            lock.readLock().lock();
            try {
                if (queueId < 1 || queueId > MessageQueue.MAX_NORMAL_QUEUE || queueId > normalQueues.size()) {
                    return null;
                }
                return normalQueues.get(queueId - 1);
            } finally {
                lock.readLock().unlock();
            }
        }

        /**
         * 清理应答过期的数据
         *
         * @param now 当前事件
         */
        public void cleanExpire(final DispatchService dispatchService, long now, final ConcurrentMap<String, ConcurrentMap<String, AtomicInteger>> expireStat) {
            /**
             * 重试队列改为了无锁，location.ownership会不停变换
             //retryQueue.cleanExpire(dispatchService, now, expireStat);
             */
            highQueue.cleanExpire(dispatchService, now, expireStat);

            boolean safeDeleted = true;
            List<DispatchQueue> removes = new ArrayList<DispatchQueue>();

            DispatchQueue queue;
            // 读锁，遍历队列，清理占用过期队列，获取要删除的队列
            lock.readLock().lock();
            try {
                // 倒叙遍历，如果中间有一个队列不能删除，则其前面的队列不能删除
                for (int i = normalQueues.size() - 1; i >= 0; i--) {
                    queue = normalQueues.get(i);
                    // 清理占用过期队列
                    queue.cleanExpire(dispatchService, now, expireStat);
                    if (safeDeleted && queue.getState().get() == QueueState.DELETED) {
                        removes.add(queue);
                    } else {
                        safeDeleted = false;
                    }
                }
            } finally {
                lock.readLock().unlock();
            }

            // 遍历删除队列
            if (!removes.isEmpty()) {
                // 写锁，确保当前不会变化队列数量，则删除状态不会改变
                lock.writeLock().lock();
                try {
                    // 倒叙遍历，如果中间有一个队列不能删除，则其前面的队列不能删除
                    for (int i = removes.size() - 1; i >= 0; i--) {
                        queue = removes.get(i);
                        if (queue.getState().get() == QueueState.DELETED) {
                            normalQueues.remove(queue);
                        } else {
                            break;
                        }
                    }
                } finally {
                    lock.writeLock().unlock();
                }
            }
        }

        protected ConcurrentMap<String, Map<String, List<Integer>>> getSequentialConsumer() {
            return this.sequentialConsumer;
        }
    }

    /**
     * 队列,ID从1开始
     */
    static class DispatchQueue {
        // 存储
        private Store store;
        // 队列
        private String topic;
        // 队列ID
        private int queueId;
        // 状态
        private AtomicReference<QueueState> state = new AtomicReference<QueueState>(QueueState.USED);
        // 最大偏移量
        private AtomicLong maxOffset = new AtomicLong(0);
        // 最小偏移量
        //private long minOffset;
        // 消费者位置
        private ConcurrentMap<String, DispatchLocation> locations = new ConcurrentHashMap<String, DispatchLocation>();

        private DispatchService service;

        public DispatchQueue(Store store, DispatchService service, String topic, short queueId) {
            if (service == null) {
                throw new IllegalArgumentException("dispatch service can not be null");
            }
            if (store == null) {
                throw new IllegalArgumentException("store can not be null");
            }
            if (topic == null || topic.isEmpty()) {
                throw new IllegalArgumentException("topic can not be empty");
            }
            if (queueId < 0 || queueId > MessageQueue.RETRY_QUEUE) {
                throw new IllegalArgumentException("queueId must be between 0 and 255");
            }
            this.service = service;
            this.topic = topic;
            this.queueId = queueId;
            this.store = store;
        }

        public String getTopic() {
            return topic;
        }

        public int getQueueId() {
            return queueId;
        }

        public boolean isBacklog(String app) {
            DispatchLocation location = locations.get(app);
            if (location != null) {
                location.isBacklog();
            }
            return false;
        }

        public long getMaxOffset() { //TODO 可能会频繁拉取
            return store.getMaxOffset(topic, getQueueId());
        }

        public void setMaxOffset(long maxOffset) {
            long cur = this.maxOffset.get();
            while (cur < maxOffset) {
                if (this.maxOffset.compareAndSet(cur, maxOffset)) {
                    break;
                } else {
                    cur = this.maxOffset.get();
                }
            }
        }

        public AtomicReference<QueueState> getState() {
            return state;
        }

        /**
         * 获取消费位置
         *
         * @param app 应用
         * @return 消费位置
         */
        public DispatchLocation getLocation(String app) {
            if (app == null) {
                return null;
            }
            DispatchLocation location = locations.get(app);

            if (location == null) {
                location = new DispatchLocation(service, this, app);
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("getLocation app=%s", app));
                }
                location.resetOffset();
                DispatchLocation old = locations.putIfAbsent(app, location);
                if (old != null) {
                    location = old;
                }
            }
            return location;
        }

        /**
         * 清理过期应答数据，检查待删除队列是否可以安全删除
         *
         * @param now 当前时间
         */
        public void cleanExpire(final DispatchService dispatchService, long now, final ConcurrentMap<String, ConcurrentMap<String, AtomicInteger>>
                expireStat) {
            DispatchLocation location;
            // 当前状态
            QueueState queueState = state.get();
            boolean safeDeleted = queueState != QueueState.USED;
            for (Map.Entry<String, DispatchLocation> entry : locations.entrySet()) {
                location = entry.getValue();
                if (location.releaseExpire(dispatchService, now)) {
                    //超时，统计超时次数
                    ConcurrentMap<String, AtomicInteger> appExpireTimes = expireStat.get(location.getTopic());
                    if (appExpireTimes == null) {
                        appExpireTimes = new ConcurrentHashMap<String, AtomicInteger>();
                        ConcurrentMap old = expireStat.putIfAbsent(location.getTopic(), appExpireTimes);
                        if (old != null) {
                            appExpireTimes = old;
                        }
                    }

                    AtomicInteger expireCounts = appExpireTimes.get(location.getApp());
                    if (expireCounts == null) {
                        expireCounts = new AtomicInteger(0);
                        AtomicInteger old = appExpireTimes.putIfAbsent(location.getApp(), expireCounts);
                        if (old != null) {
                            expireCounts = old;
                        }
                    }

                    expireCounts.incrementAndGet();
                }

                // 待删除状态，有未应答数据，有未拉取数据则不能安全删除
                if (safeDeleted && (location.hasNoAck() || location.hasRemaining())) {
                    safeDeleted = false;
                }
            }
            if (queueState == QueueState.PREPARE_DELETE && safeDeleted) {
                state.compareAndSet(QueueState.PREPARE_DELETE, QueueState.DELETING);
            } else if (queueState == QueueState.DELETING && safeDeleted) {
                state.compareAndSet(QueueState.DELETING, QueueState.DELETED);
            } else if (queueState == QueueState.DELETING && !safeDeleted) {
                state.compareAndSet(QueueState.DELETING, QueueState.PREPARE_DELETE);
            }
        }

        @Override
        public String toString() {
            return "DispatchQueue{" +
                    "topic='" + topic + '\'' +
                    ", queueId=" + queueId +
                    ", state=" + state.get() +
                    ", maxOffset=" + maxOffset +
                    //", minOffset=" + minOffset +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            DispatchQueue queue = (DispatchQueue) o;

            if (queueId != queue.queueId) return false;
            if (topic != null ? !topic.equals(queue.topic) : queue.topic != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = topic != null ? topic.hashCode() : 0;
            result = 31 * result + (int) queueId;
            return result;
        }
    }


    /**
     * 消费位置
     */
    static class DispatchLocation {
        // 消费者
        private String app;
        // 队列信息
        private DispatchQueue queue;
        // 所有者
        private AtomicReference<OwnerShip> owner = new AtomicReference<OwnerShip>();
        // 应答位置
        private long ackOffset = -1;
        // 拉取的起始位置
        private long pullBegin = -1;
        // 拉取的终止位置
        private long pullEnd = -1;
        // 下次拉取位置
        private long pullNext;
        // 上次拉取位置
        private long pullLast;
        // 队列头部时间戳和对应的offset
        private MinTimeOffset minTimeOffset = new MinTimeOffset(0l, 0l);
        //
        private DispatchService service;

        public DispatchLocation(DispatchService dispatchService, DispatchQueue queue, String app) {
            if (dispatchService == null) {
                throw new IllegalArgumentException("dispatchService can not be null");
            }
            if (queue == null) {
                throw new IllegalArgumentException("queue can not be null");
            }
            if (app == null || app.isEmpty()) {
                throw new IllegalArgumentException("app can not be empty");
            }
            this.app = app;
            this.queue = queue;
            this.service = dispatchService;
        }

        public String getTopic() {
            return queue.getTopic();
        }

        public String getApp() {
            return app;
        }

        public DispatchQueue getQueue() {
            return queue;
        }

        public AtomicReference<OwnerShip> getOwner() {
            return owner;
        }

        public long getAckOffset() {
            return ackOffset;
        }

        public long getPullBegin() {
            return pullBegin;
        }

        public long getPullEnd() {
            return pullEnd;
        }

        public long getPullNext() {
            return pullNext;
        }

        public long getPullLast() {
            return pullLast;
        }

        public MinTimeOffset getMinTimeOffset() {
            return minTimeOffset;
        }

        public void setMinTimeOffset(MinTimeOffset minTimeOffset) {
            this.minTimeOffset = minTimeOffset;
        }

        /**
         * 更新拉取位置
         *
         * @param ownerShip 所有者
         * @param pullBegin 开始位置
         * @param pullEnd   结束位置
         * @param pullNext  下次拉取位置
         * @return 成功标示
         */
        public boolean updatePullOffset(final OwnerShip ownerShip, final long pullBegin, final long pullEnd,
                                        final long pullNext) {
            synchronized (this) {
                if (this.owner.get() == ownerShip || getQueue().getQueueId() == MessageQueue.RETRY_QUEUE) {
                    this.pullLast = this.pullNext;
                    this.pullBegin = pullBegin;
                    this.pullEnd = pullEnd;
                    this.pullNext = pullNext;

                    return true;
                }
            }
            return false;
        }

        /**
         * 更新应答位置
         *
         * @param ownerShip 所有权
         * @param ackOffset 应答位置
         * @return 成功标示
         */
        public boolean updateAckOffset(final OwnerShip ownerShip, final long ackOffset) {
            synchronized (this) {
                if (this.owner.get() == ownerShip) {
                    this.ackOffset = ackOffset;
                    return true;
                }
            }
            return false;
        }

        public boolean releaseExpire(final DispatchService dispatchService, long now) {
            OwnerShip ownerShip = owner.get();
            boolean flag = release(now);
            if (flag && ownerShip != null && ownerShip.getOwner() != null) {
                dispatchService.addConsumeErr(ownerShip.getOwner(), true);
            }
            return flag;
        }

        /**
         * 是否过期的所有权
         *
         * @param now 当前时间
         * @return 成功标示
         */
        public boolean release(long now) {

            OwnerShip ownerShip = owner.get();
            // 消费者不为空
            if (ownerShip != null) {
                // 判断是否过期
                if (ownerShip.isExpire(now)) {
                    logger.warn("--now:" + now + " ,clean expire location:" + toString());
                    return release(ownerShip);
                }
            } else if (hasNoAck()) {
                logger.warn("clean has no ack location:" + toString());
                // 消费者为空还有未应答数据
                synchronized (this) {
                    if (owner.get() == null && hasNoAck()) {
                        resetOffset();
                        return true;
                    }
                }
            }

            return false;
        }

        /**
         * 清理数据
         */
        protected void resetOffset() {
            // 从存储初始化消费位置
            long offset = service.getOffset(getTopic(), getQueue().getQueueId(), getApp());
            pullNext = offset;
            ackOffset = offset >= ConsumeQueue.CQ_RECORD_SIZE ? offset - ConsumeQueue.CQ_RECORD_SIZE : -1;
            pullBegin = ackOffset;
            pullEnd = ackOffset;
            pullLast = 0;
        }

        /**
         * 释放所有权
         *
         * @param ownerShip 消费者
         * @return 成功标示
         */
        public boolean release(OwnerShip ownerShip) {
            if (ownerShip == null) {
                return false;
            }
            synchronized (this) {
                // 超时
                if (owner.compareAndSet(ownerShip, null)) {
                    resetOffset();
                    return true;
                }
            }
            return false;
        }

        /**
         * 是否有未应答数据
         *
         * @return 有未应答数据标示
         */
        public boolean hasNoAck() {
            return ackOffset < pullEnd;
        }

        /**
         * 是否还有未拉取的数据
         *
         * @return 有未拉取数据标示
         */
        public boolean hasRemaining() {
            if (queue.getQueueId() == MessageQueue.RETRY_QUEUE) {
                return true;
            }
            long maxOffset = queue.getMaxOffset();
            return maxOffset > 0 && maxOffset - pullEnd > ConsumeQueue.CQ_RECORD_SIZE;
        }

        /**
         * 是否有积压 包含未应答的数据和未拉取的数据
         *
         * @return
         */
        public boolean isBacklog() {
            return hasNoAck() || hasRemaining();
        }

        /**
         * 是否空闲
         *
         * @return 空闲标示
         */
        public boolean isFree() {
            if (queue.getQueueId() == MessageQueue.RETRY_QUEUE) {
                //重试队列也需要判断，避免长轮询每次都能拿到
                return owner.get() == null;
            }
            return hasRemaining() && !hasNoAck() && owner.get() == null;
        }

        @Override
        public String toString() {
            return "ConsumeLocation{" +
                    "app='" + app + '\'' +
                    ", queue=" + queue +
                    ", owner=" + owner.get() +
                    ", ackOffset=" + ackOffset +
                    ", pullBegin=" + pullBegin +
                    ", pullEnd=" + pullEnd +
                    ", pullNext=" + pullNext +
                    ", pullLast=" + pullLast +
                    '}';
        }
    }

    /**
     * 监听集群变化事件
     */
    protected class TopicListener implements EventListener<ClusterEvent> {
        @Override
        public void onEvent(ClusterEvent event) {
            // 确保集群已经成功起来
            if (store.isReady()) {
                try {
                    switch (event.getType()) {
                        case TOPIC_UPDATE:
                            TopicUpdateEvent notify = (TopicUpdateEvent) event;
                            TopicConfig topicConfig = notify.getTopicConfig();

                            String group = clusterManager.getBrokerGroup() != null ? clusterManager.getBrokerGroup().getGroup() : null;
                            if (group == null) {
                                logger.error("current group is null");
                                return;
                            }

                            //比较队列数
                            if (topicConfig.getQueues() != offsetManager.getQueueCount(topicConfig.getTopic())) {
                                short queues = topicConfig.getQueues();
                                if (topicConfig.checkSequential()) {
                                    queues = 1;
                                }
                                offsetManager.updateQueueCount(topicConfig.getTopic(), queues);
                            }

                            //比较消费者变化情况
                            TopicOffset topicOffset = getOffset(topicConfig.getTopic());
                            if (topicOffset != null) {
                                Set<String> consumers = topicOffset.getConsumers().keySet();
                                try {
                                    Set<String> configConsumers = topicConfig.getConsumers().keySet();

                                    for (String consumer : consumers) {
                                        if (!configConsumers.contains(consumer)) {
                                            //删除了
                                            unSubscribe(topicConfig.getTopic(), consumer);
                                        }
                                    }

                                    for (String configConsumer : configConsumers) {
                                        if (!consumers.contains(configConsumer)) {
                                            //新增了 do not check state here.
                                            //subscribe(topicConfig.getTopic(), configConsumer);
                                            offsetManager.addConsumer(topicConfig.getTopic(), configConsumer);
                                        }
                                    }
                                } catch (Exception ignored) {
                                    logger.error("", ignored);
                                }

                            }
                            break;
                    }
                } catch (JMQException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

}
