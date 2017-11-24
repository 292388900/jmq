package com.ipd.jmq.client.consumer;


import com.ipd.jmq.client.cluster.ClusterEvent;
import com.ipd.jmq.client.cluster.ClusterManager;
import com.ipd.jmq.client.connection.TransportManager;
import com.ipd.jmq.client.consumer.offset.OffsetManage;
import com.ipd.jmq.client.stat.Trace;
import com.ipd.jmq.client.stat.TraceInfo;
import com.ipd.jmq.client.stat.TracePhase;
import com.ipd.jmq.common.cluster.BrokerGroup;
import com.ipd.jmq.common.cluster.Permission;
import com.ipd.jmq.common.cluster.TopicConfig;
import com.ipd.jmq.common.model.ConsumerConfig;
import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.BrokerMessage;
import com.ipd.jmq.common.message.Message;
import com.ipd.jmq.common.message.MessageLocation;
import com.ipd.jmq.common.message.MessageQueue;
import com.ipd.jmq.common.network.v3.command.AckMessage;
import com.ipd.jmq.common.network.v3.command.JMQHeader;
import com.ipd.jmq.common.network.v3.command.RetryMessage;
import com.ipd.jmq.common.network.v3.session.ConsumerId;
import com.ipd.jmq.common.network.FailoverState;
import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.toolkit.concurrent.EventListener;
import com.ipd.jmq.toolkit.lang.Close;
import com.ipd.jmq.toolkit.retry.RetryPolicy;
import com.ipd.jmq.toolkit.service.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 主题消费者
 */
public class TopicConsumer extends Service {
    // 日志
    private static final Logger logger = LoggerFactory.getLogger(TopicConsumer.class);

    // 消费主题
    protected String topic;
    // 消费应用
    protected String app;
    // 广播客户端实例名称
    protected String clientName;
    // 过滤内容
    protected String selector;
    // 消息监听器
    protected MessageListener listener;
    // 消费配置
    protected ConsumerConfig config;
    // 消费者类型
    protected ConsumerType type;
    // 传输通道管理器
    protected TransportManager transportManager;
    // 客户端统计
    protected Trace trace;
    // broker组消费者集合
    protected ConcurrentMap<BrokerGroup, GroupConsumer> consumers = new ConcurrentHashMap<BrokerGroup, GroupConsumer>();
    // 任务调度器
    protected ScheduledExecutorService schedule;
    // 消息队列
    protected LinkedBlockingDeque<List<MessageDispatch>> messages;
    // 是否暂停
    protected AtomicBoolean paused = new AtomicBoolean(false);
    // 集群监听器
    protected TopicClusterListener clusterListener = new TopicClusterListener();
    protected MessageDispatcher dispatcher = new TopicMessageDispatcher();
    // 锁
    protected ReentrantReadWriteLock consumerLock = new ReentrantReadWriteLock();
    // 锁
    protected ReentrantLock messageLock = new ReentrantLock();
    // 本地offset管理
    private OffsetManage localOffsetManage;

    public TopicConsumer(String topic, String selector, MessageListener listener) {
        setTopic(topic);
        setSelector(selector);
        setListener(listener);
    }

    public void setTransportManager(TransportManager transportManager) {
        this.transportManager = transportManager;
    }

    public void setSchedule(ScheduledExecutorService schedule) {
        this.schedule = schedule;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setSelector(String selector) {
        this.selector = selector;
    }

    public void setListener(MessageListener listener) {
        this.listener = listener;
    }

    public void setConfig(ConsumerConfig config) {
        this.config = config;
    }

    public ConsumerType getType() {
        return type;
    }

    public void setType(ConsumerType type) {
        this.type = type;
    }

    @Override
    protected void validate() throws Exception {
        super.validate();
        if (topic == null || topic.isEmpty()) {
            throw new IllegalStateException("topic can not be empty");
        }
        if (transportManager == null) {
            throw new IllegalStateException("transportManager can not be null");
        }
        if (schedule == null) {
            throw new IllegalStateException("schedule can not be null");
        }
        if (config == null) {
            throw new IllegalStateException("config can not be null");
        }
        if (clientName == null) {
            clientName = app;
        }
        if (type == ConsumerType.LISTENER && listener == null) {
            throw new IllegalStateException("listener is null");
        } else if (type == ConsumerType.PULL && messages == null) {
            messages = new LinkedBlockingDeque<List<MessageDispatch>>(config.getPrefetchSize());
        }
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        trace = transportManager.getTrace();
        transportManager.addListener(topic, clusterListener);
        logger.info(String.format("consumer is started topic:%s app:%s.", topic, transportManager.getConfig().getApp()));
    }

    @Override
    protected void doStop() {
        super.doStop();
        consumerLock.writeLock().lock();
        try {
            for (GroupConsumer consumer : consumers.values()) {
                Close.close(consumer);
            }
            transportManager.removeTransports(topic, Permission.READ);
            transportManager.removeListener(topic, clusterListener);
            consumers.clear();
            if (messages != null) {
                messages.clear();
            }
        } finally {
            consumerLock.writeLock().unlock();
        }
        logger.info(String.format("consumer %s is stopped.", topic));
    }

    /**
     * 暂停所有消费者
     */
    public void pause() {
        if (paused.compareAndSet(false, true)) {
            consumerLock.readLock().lock();
            try {
                if (!isStarted()) {
                    return;
                }
                for (GroupConsumer consumer : consumers.values()) {
                    consumer.pause();
                }
                logger.info(String.format("consumer %s is paused.", topic));
            } finally {
                consumerLock.readLock().unlock();
            }
        }
    }

    /**
     * 是否暂停
     *
     * @return
     */
    public boolean isPaused() {
        return paused.get();
    }

    /**
     * 恢复所有消费者
     */
    public void resume() {
        if (paused.compareAndSet(true, false)) {
            consumerLock.readLock().lock();
            try {
                if (!isStarted()) {
                    return;
                }
                for (GroupConsumer consumer : consumers.values()) {
                    consumer.resume();
                }
                logger.info(String.format("consumer %s is resumed.", topic));
            } finally {
                consumerLock.readLock().unlock();
            }
        }
    }

    /**
     * 拉取指定数量消息
     * 防止缓冲区消息乱序，synchronized
     *
     * @return 实际拉取的数量
     */
    public int pull(final MessageListener listener) {
        if (!isStarted()) {
            return 0;
        }
        try {
            // 只包含一个分组的数据
            List<MessageDispatch> dispatches = messages.poll(getUpdatedPullTimeout(), TimeUnit.MILLISECONDS);
            if (dispatches == null || dispatches.isEmpty()) {
                return 0;
            }

            //如果有消息了，说明已经获取到topicConfig
            TopicConfig topicConfig = transportManager.getClusterManager().getTopicConfig(topic);
            if (topicConfig == null) {
                messages.addFirst(dispatches);
                logger.error("topicConfig is null " + topic);
                return 0;
            }
            if (topicConfig.isLocalManageOffsetConsumer(app)) {
                throw new RuntimeException("localManageOffset not support pull()!");
            }
            dispatchMessages(dispatches, listener, topicConfig);
            return dispatches.size();
        } catch (InterruptedException e) {
            return 0;
        }
    }

    /**
     * 拉取指定数量消息
     * 防止缓冲区消息乱序，synchronized
     *
     * @return 实际拉取的数量
     */
    public List<MessageDispatch> pull() {
        if (!isStarted()) {
            return Collections.emptyList();
        }
        try {
            // 只包含一个分组的数据
            List<MessageDispatch> dispatches = messages.poll(getUpdatedPullTimeout(), TimeUnit.MILLISECONDS);
            if (dispatches == null || dispatches.isEmpty()) {
                return Collections.emptyList();
            }
            return dispatches;
        } catch (InterruptedException e) {
            return Collections.emptyList();
        }
    }

    public int pullFromBroker(ConsumerStrategy consumerStrategy) throws JMQException {
        if (!isStarted()) {
            return 0;
        }
        ClusterManager clusterManager = transportManager.getClusterManager();
        byte dataCenter = clusterManager.getDataCenter();
        TopicConfig topicConfig = clusterManager.getTopicConfig(topic);
        if (topicConfig == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("topicConfig is not init");
            }
            checkInit(topicConfig, topic, getUpdatedPullTimeout());
        }
        List<BrokerGroup> brokerGroups = new ArrayList<BrokerGroup>();
        for (Map.Entry<BrokerGroup, GroupConsumer> entry : consumers.entrySet()) {
            brokerGroups.add(entry.getKey());
        }

        String groupName = consumerStrategy.electBrokerGroup(topicConfig, dataCenter);
        short queueId = consumerStrategy.electQueueId(groupName, topicConfig, dataCenter);
        try {
            GroupConsumer consumer = null;
            for (Map.Entry<BrokerGroup, GroupConsumer> entry : consumers.entrySet()) {
                if (groupName.equals(entry.getKey().getGroup())) {
                    consumer = entry.getValue();
                    break;
                }
            }
            if (consumer != null && consumer.isStarted()) {
                long offset = -1;
                if (topicConfig != null && topicConfig.isLocalManageOffsetConsumer(app)) {
                    if (queueId <= 0 || (queueId > topicConfig.getQueues() && queueId != MessageQueue.HIGH_PRIORITY_QUEUE)) {
                        throw new JMQException(String.format("queueId:%d is invalid", queueId), JMQCode.CN_PARAM_ERROR.getCode());
                    }
                    offset = localOffsetManage.getOffset(clientName, groupName, topic, queueId);
                }
                if (consumer.getTransport().getState() == null || consumer.getTransport().getState() != FailoverState.CONNECTED) {
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException ignored) {

                    }
                }
                return consumer.pull(queueId, offset);
            } else {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException ignored) {

                }
            }
        } finally {
            consumerStrategy.pullEnd(groupName, queueId);
        }
        return 0;
    }

    /**
     * 检查连接是否可用
     * @param topicConfig 主题配置
     * @param topic 主题
     * @param pullTimeout 拉取超时时间
     */
    protected void checkInit(final TopicConfig topicConfig, final String topic, final long pullTimeout) {
        if (topicConfig != null) {
            return;
        }
        // 每次sleep时间和循环检测次数
        long sleepTime = 100;
        int count = (int) (pullTimeout / sleepTime);
        if (count < 5) {
            count = 10;
            sleepTime = pullTimeout / count;
            if (sleepTime < 10) {
                sleepTime = 10;
            }
        }
        for (int i = 0; i < count; i++) {
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                throw new RuntimeException("获取配置信息失败", e);
            }
            TopicConfig config = transportManager.getClusterManager().getTopicConfig(topic);
            if (config != null) {
                return;
            }
        }

    }

    protected void localAcknowledge(List<MessageDispatch> dispatches) {
        if (dispatches == null || dispatches.isEmpty()) {
            return;
        }
        for (int i = 0; i < dispatches.size(); i++) {
            localOffsetManage.updateOffset(clientName,
                    dispatches.get(i).getGroup().getGroup(),
                    topic,
                    dispatches.get(i).getMessage().getQueueId(),
                    dispatches.get(i).getMessage().getQueueOffset() + 22,
                    false,
                    true);
        }

    }

    /**
     * 应答消息，目前消息来源于同一个Broker下的同一个消费者
     *
     * @param dispatches 待应答消息
     */
    protected void acknowledge(List<MessageDispatch> dispatches) {
        if (dispatches == null || dispatches.isEmpty()) {
            return;
        }
        int count = 0;
        ConsumerId consumerId = null;
        BrokerGroup group = null;

        // 遍历构造消息位置列表
        MessageLocation[] locations = new MessageLocation[dispatches.size()];
        for (MessageDispatch dispatch : dispatches) {
            if (count == 0) {
                consumerId = dispatch.getConsumerId();
                group = dispatch.getGroup();
            }
            locations[count++] = new MessageLocation(dispatch.getMessage());
        }
        // 构造应答消息
        AckMessage ackMessage = new AckMessage().consumerId(consumerId).locations(locations);

        // 取得分组的消费者
        GroupConsumer consumer = consumers.get(group);
        if (consumer != null) {
            try {
                // 应答失败，会自动重试
                consumer.acknowledge(ackMessage);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    /**
     * 应答消息，目前消息来源于同一个Broker下的同一个消费者
     *
     * @param consumerId  消费者ID
     * @param brokerGroup 分组
     * @param locations   位置
     * @throws JMQException
     */
    public void acknowledge(final String consumerId, final String brokerGroup,
                            final MessageLocation[] locations, final boolean sync) throws JMQException {
        if (locations == null || locations.length == 0) {
            return;
        }
        if (consumerId == null || consumerId.isEmpty()) {
            throw new JMQException("consumerId can not be empty.", JMQCode.CN_CONNECTION_ERROR.getCode());
        }
        if (brokerGroup == null || brokerGroup.isEmpty()) {
            throw new JMQException("brokerGroup can not be empty.", JMQCode.CN_CONNECTION_ERROR.getCode());
        }
        // 构造应答消息
        AckMessage ackMessage = new AckMessage().consumerId(new ConsumerId(consumerId)).locations(locations);
        // 取得分组的消费者
        GroupConsumer consumer = null;
        for (Map.Entry<BrokerGroup, GroupConsumer> e : consumers.entrySet()) {
            if (e.getKey().getGroup().equals(brokerGroup)) {
                consumer = e.getValue();
            }
        }
        if (consumer != null) {
            // 应答失败，会自动重试
            if (!sync) {
                consumer.acknowledge(ackMessage);
            } else {
                Command response = consumer.syncAcknowledge(ackMessage);
                JMQHeader header = (JMQHeader) response.getHeader();
                if (header.getStatus() != JMQCode.SUCCESS.getCode()) {
                    throw new JMQException(header.getError(), header.getStatus());
                }
            }
        } else {
            throw new JMQException(
                    String.format("consumer is not found, consumerId=%s, group=%s", consumerId, brokerGroup),
                    JMQCode.CN_CONNECTION_ERROR.getCode());
        }
    }

    public void acknowledge(final String consumerId, final String brokerGroup,
                            final MessageLocation[] locations) throws Exception {
        acknowledge(consumerId, brokerGroup, locations, false);
    }

    /**
     * 重试消息，目前只包含一组的数据
     *
     * @param consumerId  消费者ID
     * @param brokerGroup 分组
     * @param locations   位置
     * @param exception   异常
     * @throws JMQException
     */
    public void retry(String consumerId, String brokerGroup,
                      MessageLocation[] locations, String exception) throws JMQException {
        if (locations == null || locations.length == 0) {
            return;
        }
        if (consumerId == null || consumerId.isEmpty()) {
            throw new JMQException("consumerId can not be empty.", JMQCode.CN_CONNECTION_ERROR.getCode());
        }
        if (brokerGroup == null || brokerGroup.isEmpty()) {
            throw new JMQException("brokerGroup can not be empty.", JMQCode.CN_CONNECTION_ERROR.getCode());
        }
        // 构造重试消息
        RetryMessage retryMessage =
                new RetryMessage().consumerId(new ConsumerId(consumerId)).locations(locations).exception(exception);
        // 取得分组的消费者
        GroupConsumer consumer = null;
        for (Map.Entry<BrokerGroup, GroupConsumer> e : consumers.entrySet()) {
            if (e.getKey().getGroup().equals(brokerGroup)) {
                consumer = e.getValue();
            }
        }
        if (consumer != null) {
            // 应答失败，会自动重试
            Command response = consumer.retry(retryMessage);
            JMQHeader header = (JMQHeader) response.getHeader();
            if (header.getStatus() != JMQCode.SUCCESS.getCode()) {
                throw new JMQException(header.getError(), header.getStatus());
            }
        } else {
            throw new JMQException(
                    String.format("consumer is not found, consumerId=%s, group=%s", consumerId, brokerGroup),
                    JMQCode.CN_CONNECTION_ERROR.getCode());
        }
    }

    /**
     * 重试消息，目前只包含一组的数据
     *
     * @param dispatches 待重试消息
     * @param exception  异常
     */
    protected void retry(List<MessageDispatch> dispatches, Exception exception) {
        if (dispatches == null || dispatches.isEmpty()) {
            return;
        }
        ConsumerId consumerId = null;
        BrokerGroup group = null;
        int count = 0;
        final TraceInfo info = new TraceInfo(topic, trace.getApp(), TracePhase.SENDRETRY);
        trace.begin(info);
        // 遍历构造消息位置列表
        MessageLocation[] locations = new MessageLocation[dispatches.size()];
        for (MessageDispatch dispatch : dispatches) {
            if (count == 0) {
                consumerId = dispatch.getConsumerId();
                group = dispatch.getGroup();
            }
            locations[count++] = new MessageLocation(dispatch.getMessage());
        }
        // 构造重试消息
        RetryMessage retryMessage =
                new RetryMessage().consumerId(consumerId).locations(locations).exception(getError(exception));
        // 取得分组的消费者
        GroupConsumer consumer = consumers.get(group);
        if (consumer != null) {
            try {
                Command response = null;
                // 重试失败，下次会自动从当前位置拉取数据
                response = consumer.retry(retryMessage);

                if (null != response) {
                    JMQHeader header = (JMQHeader) response.getHeader();
                    if (header.getStatus() != JMQCode.SUCCESS.getCode()) {
                        throw new JMQException(header.getError(), header.getStatus());
                    }
                }
                info.count(count).success();
                trace.end(info);
            } catch (JMQException e) {
                info.count(count).error(e);
                trace.end(info);
                logger.error(e.getMessage(), e);
            }
        } else {
            info.count(count).error(new Exception("group consumer is null"));
            trace.end(info);
            logger.error(String.format("consumer [topic:%s,app:%s,group:%s] is empty", topic, transportManager.getConfig().getApp(), group));
        }

    }

    /**
     * 获取异常
     *
     * @param e 异常
     * @return 异常字符串
     */
    protected String getError(Exception e) {
        if (e == null) {
            return null;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(out);
        try {
            e.printStackTrace(writer);
            writer.flush();
            String errMsg = out.toString();
            // 强制限制异常信息的长度
            if (errMsg != null && errMsg.length() > 1024) {
                errMsg = errMsg.substring(1024);
            }
            return errMsg;
        } finally {
            writer.close();
        }
    }

    /**
     * 增加分组消费者
     *
     * @param group  分组
     * @param queues 队列数
     */
    protected void addGroupConsumer(BrokerGroup group, short queues) {
        if (group == null) {
            return;
        }
        consumerLock.writeLock().lock();
        try {
            if (!isStarted()) {
                return;
            }
            // 判断消费组是否已经创建
            GroupConsumer consumer = consumers.get(group);
            if (consumer != null) {
                return;
            }
            // 创新新消费组
            consumer = new GroupConsumer(topic, group, selector, queues);
            consumer.setConfig(config);
            consumer.setTransportManager(transportManager);
            consumer.setSchedule(schedule);
            consumer.setDispatcher(dispatcher);
            consumer.setQueues(queues);
            consumer.setLocalOffsetManage(localOffsetManage);
            consumer.setApp(app);
            consumer.setClientName(clientName);
            consumer.setType(type);
            // 是否已经暂停消费
            if (paused.get()) {
                consumer.pause();
            }
            // 启动消费组
            consumer.start();
            consumers.put(group, consumer);
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("add groupConsumer topic:%s,group:%s", topic, group.getGroup()));
            }
        } catch (Exception ignored) {
            logger.error("", ignored);
            //不会抛出异常
        } finally {
            consumerLock.writeLock().unlock();
        }
    }

    /**
     * 删除分组消费者
     *
     * @param group 分组
     */
    protected void removeGroupConsumer(BrokerGroup group) {
        consumerLock.writeLock().lock();
        try {
            if (!isStarted()) {
                return;
            }
            GroupConsumer consumer = consumers.remove(group);
            if (consumer != null) {
                Close.close(consumer);
            }
        } finally {
            consumerLock.writeLock().unlock();
        }
    }

    /**
     * queue变化通知group消费者
     *
     * @param queues 队列数量
     */
    protected void updateQueues(short queues) {
        if (consumers.isEmpty()) {
            logger.warn(String.format("topic:%s groupConsumer is empty", topic));
            return;
        }
        consumerLock.readLock().lock();
        try {
            if (!isStarted()) {
                return;
            }
            for (GroupConsumer consumer : consumers.values()) {
                consumer.updateQueues(queues);
            }
        } finally {
            consumerLock.readLock().unlock();
        }
    }

    /**
     * queue变化通知group消费者
     *
     * @param queues 队列数量
     */
    protected void updateTopicQueues(short queues) {
        if (consumers.isEmpty()) {
            logger.warn(String.format("topic:%s groupConsumer is empty", topic));
            return;
        }
        consumerLock.readLock().lock();
        try {
            if (!isStarted()) {
                return;
            }
            for (GroupConsumer consumer : consumers.values()) {
                consumer.updateTopicQueues(queues);
            }
        } finally {
            consumerLock.readLock().unlock();
        }
    }

    /**
     * 分发消息，消息来源于同一个Broker下的同一个消费者
     *
     * @param dispatches 待分发的消息
     * @param listener   监听器
     * @param topicConfig  主题配置
     */
    protected boolean dispatchMessages(final List<MessageDispatch> dispatches, final MessageListener listener, final TopicConfig topicConfig) {
        if (dispatches == null || dispatches.isEmpty() || listener == null) {
            return false;
        }
        // 监听器模式
        final List<Message> messages = new ArrayList<Message>();
        for (MessageDispatch dispatch : dispatches) {
            messages.add(dispatch.getMessage());
        }
        final TraceInfo info = new TraceInfo(topic, trace.getApp(), TracePhase.CONSUME);
        trace.begin(info);
        final int count = messages.size();

        config = transportManager.getClusterManager().getConsumerConfig(config, app, topic);
        RetryPolicy retryPolicy = config.getRetryPolicy();

        // 本地重试
        try {
            Boolean resultFlag = ConsumerRetryLoop.execute(retryPolicy, new ConsumerRetryLoop.RetryCallable<Boolean>() {
                @Override
                public boolean onException(Exception e) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("consume error retry", e);
                    }
                    return true;
                }

                @Override
                public boolean shouldContinue() {
                    return isStarted();
                }

                @Override
                public Boolean call() throws Exception {
                    StringBuffer bizIds = null;
                    StringBuffer msgIds = null;
                    try {
                        if (logger.isDebugEnabled()) {
                            bizIds = new StringBuffer();
                            msgIds = new StringBuffer();
                            for (Message message : messages) {
                                if (bizIds.length() != 0) {
                                    bizIds.append(",");
                                    bizIds.append(message.getBusinessId());
                                } else {
                                    bizIds.append(message.getBusinessId());
                                }

                                if (msgIds.length() != 0) {
                                    msgIds.append(",");
                                    msgIds.append(((BrokerMessage) message).getMessageId());
                                } else {
                                    msgIds.append(((BrokerMessage) message).getMessageId());
                                }
                            }
                            logger.debug(String.format("Begin to process messages.messageIds:%s, businessIds:%s",
                                    msgIds, bizIds));
                        }
                    } catch (Exception e) {
                        logger.warn("");
                    }
                    listener.onMessage(messages);
                    // 消费成功，应答消息
                    try {
                        if (topicConfig.isLocalManageOffsetConsumer(app)) {
                            localAcknowledge(dispatches);
                        } else {
                            if (logger.isDebugEnabled()) {
                                logger.debug(String.format("Message process finished! will send ACK.messageIds:%s, " +
                                                "businessIds:%s",
                                        msgIds, bizIds));
                            }
                            acknowledge(dispatches);
                        }
                        if (logger.isDebugEnabled()) {
                            logger.debug(String.format("ACK finished. messageIds:%s, businessIds:%s", msgIds, bizIds));
                        }
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                    // 增加消费统计
                    info.count(count).success();
                    trace.end(info);
                    return Boolean.TRUE;
                }
            });
            if (resultFlag != null && resultFlag) {
                return true;
            }
        } catch (Throwable err) {
            if (topicConfig.isLocalManageOffsetConsumer(app)) {
                logger.error("consumer error, please retry the message", err);
            } else {
                logger.error("consumer error,put to retry queue.", err);
                // 消费失败，增加消费统计
                info.count(count).error(err);
                trace.end(info);
                // 本地重试失败，则提交服务端进行重试，服务端会自动进行确认
                Exception ex = new Exception(err);
                retry(dispatches, ex);
            }
        }
        return false;
    }

    /**
     * 主题消费者集群监听器,监听Topic上Broker组的变化
     */
    protected class TopicClusterListener implements EventListener<ClusterEvent> {

        @Override
        public void onEvent(ClusterEvent event) {
            if (!isStarted() || !topic.equals(event.getTopic())) {
                return;
            }

            BrokerGroup group = event.getGroup();
            switch (event.getType()) {
                case ADD_BROKER:
                    // 能读数据
                    if (group.getPermission().contain(Permission.READ)) {
                        addGroupConsumer(group, event.getQueues());
                    }
                    break;
                case REMOVE_BROKER:
                    removeGroupConsumer(group);
                    break;
                case UPDATE_BROKER:
                    // 能读数据
                    if (group.getPermission().contain(Permission.READ)) {
                        addGroupConsumer(group, event.getQueues());
                    } else {
                        removeGroupConsumer(group);
                    }
                    break;
                case QUEUE_CHANGE:
                    if (topic.equals(event.getTopic())) {
                        updateQueues(event.getQueues());
                    }
                    break;
                case TOPIC_QUEUE_CHANGE:
                    if (topic.equals(event.getTopic())) {
                        updateTopicQueues(event.getQueues());
                    }
                    break;
            }

        }
    }

    /**
     * 消息派发
     */
    protected class TopicMessageDispatcher implements MessageDispatcher {
        @Override
        public boolean dispatch(final List<MessageDispatch> dispatches, final TopicConfig topicConfig) {
            if (type == ConsumerType.PULL) {
                messageLock.lock();
                try {
                    // 拉模式，放到内存队列
                    try {
                        messages.put(dispatches);
                    } catch (InterruptedException e) {
                        logger.warn(e.getMessage());
                    }
                } finally {
                    messageLock.unlock();
                }
                return true;
            } else {
                // 派发消息
                return dispatchMessages(dispatches, listener, topicConfig);
            }
        }
    }

    public void flush() {
         localOffsetManage.flush(topic,clientName);
    }

    public ConsumerConfig getConfig() {
        return config;
    }

    public void setLocalOffsetManage(OffsetManage localOffsetManage) {
        this.localOffsetManage = localOffsetManage;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    private int getUpdatedPullTimeout() {
        config = transportManager.getClusterManager().getConsumerConfig(config, app, topic);
        return config.getPullTimeout();
    }

}
