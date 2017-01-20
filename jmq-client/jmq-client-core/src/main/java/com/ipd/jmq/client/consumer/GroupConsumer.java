package com.ipd.jmq.client.consumer;

import com.ipd.jmq.client.connection.ConsumerClient;
import com.ipd.jmq.client.connection.ConsumerTransport;
import com.ipd.jmq.client.connection.TransportManager;
import com.ipd.jmq.client.consumer.offset.AllocateQueuePolling;
import com.ipd.jmq.client.consumer.offset.OffsetManage;
import com.ipd.jmq.client.exception.ExceptionHandler;
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
import com.ipd.jmq.common.network.v3.command.*;
import com.ipd.jmq.common.network.v3.session.ConsumerId;
import com.ipd.jmq.common.network.FailoverState;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.model.Acknowledge;
import com.ipd.jmq.common.network.command.Command;
import com.ipd.jmq.toolkit.concurrent.EventListener;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.time.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 分组消费者
 */
public class GroupConsumer extends Service {
    // 守护进程初始检测时间
    public static final int GUARD_INITIAL_DELAY = 1000 * 2;
    // 守护进程检测时间
    public static final int GUARD_INTERVAL = 1000 * 30;
    // 没有拉取数据的最大休息时间，需要小于守护进程检测时间，确保一个检测周期里面拉取了一次数据
    public static final int MAX_PULL_EMPTY_SLEEP_TIME = 1000 * 20;
    // 快速增加消费线程数量
    public static final int FAST_INC_COUNT = 2;
    // 慢慢增加消费线程数量
    public static final int SLOW_INC_COUNT = 1;
    // 快速增加消费线程阀值，队列数与活消费动线程数的差值大于该阀值，则快速增加消费线程
    public static final int FAST_INC_THRESHOLD = 5;
    // 日志
    private static final Logger logger = LoggerFactory.getLogger(GroupConsumer.class);
    // 最大连续空闲时间
    private static final int MAX_IDLE_TIMES = 5;
    // 主题
    protected String topic;
    // 应用
    protected String app;

    // 消费者类型
    protected ConsumerType type;

    // 广播客户端实例名称
    protected String clientName;
    // 选择器
    protected String selector;
    // 监听器
    protected MessageDispatcher dispatcher;
    // 消费配置
    protected ConsumerConfig config;
    // broker组
    protected BrokerGroup group;
    // 传输通道
    protected ConsumerTransport transport;
    // 消费者
    protected List<QueueConsumer> consumers = new ArrayList<QueueConsumer>();
    // 连接管理器
    protected TransportManager transportManager;
    // 客户端统计
    protected Trace trace;
    // 线程调度器
    protected ScheduledExecutorService schedule;
    // 是否暂停
    protected AtomicBoolean paused = new AtomicBoolean(false);
    // 线程组
    protected ThreadGroup threadGroup;
    // queue数量，消费者数量不超过queue数量，用于控制线程数,这个数量与topicQueues不一致
    protected short queues;
    protected AllocateQueuePolling queuePolling = new AllocateQueuePolling();
    // 本地offset管理
    private OffsetManage localOffsetManage;
    // queues队列
    private AtomicLong pullTotal = new AtomicLong(0);
    // 队列访问记数
    private AtomicLong queueTimes = new AtomicLong(0);
    // 记录控制打印日志异常的时间戳
    protected ConcurrentMap<Integer, Long> controlPrintLogMap = new ConcurrentHashMap<Integer, Long>();
    // 失败重试监听
    private FailoverStateListener failoverStateListener = new FailoverStateListener();


    public GroupConsumer(String topic, BrokerGroup group, String selector, short queues) {
        setTopic(topic);
        setGroup(group);
        setSelector(selector);
        setQueues(queues);
    }


    @Override
    protected void validate() throws Exception {
        super.validate();
        this.controlPrintLogMap.put(JMQCode.FW_GET_MESSAGE_TOPIC_NOT_READ.getCode(), 0L);
        this.controlPrintLogMap.put(JMQCode.FW_GET_MESSAGE_APP_CLIENT_IP_NOT_READ.getCode(), 0L);
        if (topic == null || topic.isEmpty()) {
            throw new IllegalStateException("topic can not be empty");
        }
        if (app == null || app.isEmpty()) {
            throw new IllegalStateException("app can not be empty");
        }
        if (group == null) {
            throw new IllegalStateException("group can not be null");
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
        if (transportManager.getClusterManager().getTopicConfig(topic) == null) {
            throw new IllegalStateException("topicConfig should not be empty:" + topic);
        }
        if (localOffsetManage == null) {
            if (transportManager.getClusterManager().getTopicConfig(topic).isLocalManageOffsetConsumer(app)) {
                throw new IllegalStateException("BROADCAST topic localOffsetManage can not be empty:" + topic);
            }
        }
        if (config.getPullTimeout() > 0 && config.getLongPull() > 0 && config.getPullTimeout() < config.getLongPull()) {
            throw new IllegalStateException(
                    String.format("config.longPull:%d can not be greater than config.pullTimeout:%d",
                            config.getLongPull(), config.getPullTimeout()));
        }
        // 判断连接
        if (transport == null) {
            // 创建连接
            transport = (ConsumerClient) transportManager.getTransport(group, topic, Permission.READ);
            transport.setSelector(selector);
        }
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        threadGroup = new ThreadGroup(String.format("consumer_%s_%s", topic, group.getGroup()));
        ((ConsumerClient) transport).start();
        transport.addListener(failoverStateListener);
        trace = transportManager.getTrace();
        queuePolling.allocate(transportManager.getClusterManager().getTopicConfig(topic).getQueues());
        schedule.schedule(new ConsumerGuard(), GUARD_INITIAL_DELAY, TimeUnit.MILLISECONDS);
        // BUG 停止的时候没有中断调度
        // schedule.scheduleWithFixedDelay(new ConsumerGuard(), GUARD_INITIAL_DELAY, GUARD_INTERVAL, TimeUnit
        // .MILLISECONDS);
        logger.info(String.format("consumer [topic:%s app:%s] to %s is started.", topic, transportManager.getConfig().getApp(), group.toString()));
    }

    @Override
    protected void doStop() {
        super.doStop();
        threadGroup.interrupt();
        threadGroup = null;
        // 在锁里面，没用并发问题
        for (QueueConsumer consumer : consumers) {
            consumer.close();
        }
        consumers.clear();
        if (transport != null) {
            transport.stop();
            transport = null;
        }
        logger.info(String.format("consumer %s to %s is stopped.", topic, group.toString()));
    }

    /**
     * 暂停
     */
    public void pause() {
        if (paused.compareAndSet(false, true)) {

        }
    }

    /**
     * 恢复
     */
    public void resume() {
        if (paused.compareAndSet(true, false)) {

        }
    }

    /**
     * 应答,失败会进入重试队列
     *
     * @param command 应答命令
     * @throws JMQException
     */
    public void acknowledge(final AckMessage command) throws JMQException {
        if (command == null) {
            return;
        }
        try {
            transport.oneway(new Command(JMQHeader.Builder.request(command.type(), Acknowledge.ACK_NO), command));
        } catch (TransportException e) {
            throw new JMQException(e.getMessage(), e.getCode());
        }
    }

    /**
     * 应答,失败会进入重试队列
     *
     * @param command 应答命令
     * @throws JMQException
     */
    public Command syncAcknowledge(final AckMessage command) throws JMQException {
        if (command == null) {
            return null;
        }
        try {
            return transport.sync(new Command(JMQHeader.Builder.request(command.type(), Acknowledge.ACK_RECEIVE), command));
        } catch (TransportException e) {
            throw new JMQException(e.getMessage(), e.getCode());
        }
    }

    /**
     * 重试
     *
     * @param command 重试命令
     * @throws JMQException
     */
    public Command retry(final RetryMessage command) throws JMQException {
        try {
            return transport.sync(new Command(JMQHeader.Builder.request(command.type(), Acknowledge.ACK_RECEIVE), command));
        } catch (TransportException e) {
            throw new JMQException(e.getMessage(), e.getCode());
        }
    }

    /**
     * 更新队列数
     *
     * @param queues 队列数
     */
    public void updateQueues(short queues) {
        // 判断是否发生变更
        if (queues < 0 || this.queues == queues) {
            return;
        }
        // 在下一个调度周期里面进行消费线程变更
        this.queues = queues;
    }

    /**
     * 更新队列数
     *
     * @param queues 队列数
     */
    public void updateTopicQueues(short queues) {
        queuePolling.resetQueues(queues);
    }

    /**
     * 创建消费线程
     *
     * @param consumer 消费者
     */
    protected void createThread(QueueConsumer consumer) {
        if (consumer == null) {
            return;
        }
        String name = "consumer_" + topic + "_" + group.getGroup() + "_" + SystemClock.now();
        Thread thread = new Thread(threadGroup, consumer, name);
        thread.start();
        consumer.setThread(thread);
    }

    /**
     * 拉取消息
     *
     * @return 消息条数
     * @throws JMQException
     */
    protected int pull(short queueId, long offset) throws JMQException {
        if (transport.getState() == FailoverState.DISCONNECTED) {
            return 0;
        }
        ConsumerId consumerId = transport.getConsumerId();
        if (consumerId == null) {
            return 0;
        }
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("start pull topic:%s", topic));
        }

        // 开启性能统计
        TraceInfo traceInfo = new TraceInfo(topic, trace.getApp(), TracePhase.RECEIVE);
        trace.begin(traceInfo);

        config = transportManager.getClusterManager().getConsumerConfig(config, app, topic);
        int updatedLongPull = config.getLongPull();
        int updatedPullTimeout = config.getPullTimeout();

        // 拉取数据
        GetMessage getMessage = new GetMessage()
                .topic(topic)
                .consumerId(consumerId)
                .longPull(updatedLongPull)
                .queueId(queueId)
                .offset(offset);
        Command request = new Command(JMQHeader.Builder.request(getMessage.type(), Acknowledge.ACK_RECEIVE), getMessage);
        Command response;
        try {
            response = transport.sync(request, updatedPullTimeout);
            JMQHeader header = (JMQHeader) response.getHeader();
            int status = header.getStatus();
            if (status != JMQCode.SUCCESS.getCode()) {
                throw new JMQException(header.getError(), status);
            }
        } catch (JMQException e) {
            // 出错
            traceInfo.count(0).size(0).error(e);
            trace.end(traceInfo);
            throw e;
        } catch (TransportException e) {
            traceInfo.count(0).size(0).error(e);
            trace.end(traceInfo);
            throw new JMQException(e.getMessage(), e.getCode());
        }

        long size = 0;
        GetMessageAck ack = (GetMessageAck) response.getPayload();
        BrokerMessage[] messages = ack.getMessages();
        if (messages == null || messages.length == 0) {
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("pull %d message", 0));
            }
            // 没有取到数据
            traceInfo.count(0).size(0).success();
            trace.end(traceInfo);
            return 0;
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("pull %d message", messages.length));
            }
            // 统计数据包大小，并构造消息派发
            List<MessageDispatch> dispatches = new ArrayList<MessageDispatch>();
            for (BrokerMessage message : messages) {
                size += message.getSize();
                dispatches.add(new MessageDispatch(message, consumerId, group));
            }
            // 结束性能统计
            traceInfo.count(messages.length).size(size).success();
            trace.end(traceInfo);

            // 派发消息
            long mid = SystemClock.now();
            dispatcher.dispatch(dispatches, transportManager.getClusterManager().getTopicConfig(topic));
            pullTotal.incrementAndGet();
            if (logger.isDebugEnabled()) {
                if (pullTotal.get() % 10000 == 0) {
                    logger.debug(String.format(
                            "pull message from server and ack to server time:%d ms,pull time:%d,ack time:%d," +
                                    "" + "message size:%d,message byte len:%d,pull times:%d",
                            traceInfo.getElapsedTime(), mid - traceInfo.getStartTime(), traceInfo.getEndTime() - mid,
                            messages.length, size, pullTotal.get()));
                }
            }
            return messages.length;
        }
    }

    public void setConfig(ConsumerConfig config) {
        this.config = config;
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

    public void setGroup(BrokerGroup group) {
        this.group = group;
    }

    public void setQueues(short queues) {
        if (queues >= 0) {
            this.queues = queues;
        }
    }

    public ConsumerType getType() {
        return type;
    }

    public void setType(ConsumerType type) {
        this.type = type;
    }

    public void setDispatcher(MessageDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    public void setTransportManager(TransportManager transportManager) {
        this.transportManager = transportManager;
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

    public ConsumerTransport getTransport() {
        return transport;
    }

    public void setTransport(ConsumerTransport transport) {
        this.transport = transport;
    }

    /**
     * 拉取统计信息
     */
    protected class PullStat {
        // 连续空闲次数
        protected AtomicInteger idleTimes = new AtomicInteger(0);
        // 时间片段里面有数据次数
        protected AtomicInteger busyTimes = new AtomicInteger(0);
        // 时间片段里面调用字数
        protected AtomicInteger pullTimes = new AtomicInteger(0);

        public PullStat() {
        }

        public PullStat(PullStat stat) {
            if (stat != null) {
                idleTimes.set(stat.idleTimes.get());
                busyTimes.set(stat.busyTimes.get());
                pullTimes.set(stat.pullTimes.get());
            }
        }

        /**
         * 是否空闲
         *
         * @return 空闲标示
         */
        public boolean isIdle() {
            // 连续空闲次数大于指定阀值
            return idleTimes.get() >= MAX_IDLE_TIMES;
        }

        /**
         * 是否忙
         *
         * @return 忙标示
         */
        public boolean isBusy() {
            // 指定时间段里面，每次拉取都有数据
            int busyCount = busyTimes.get();
            int pullCount = pullTimes.get();
            return pullCount == 0 || pullCount == busyCount;
        }

        /**
         * 更新数据
         *
         * @param count 本次拉取的消息条数
         */
        public void update(final int count) {
            if (count <= 0) {
                // 没有拉取到消息
                pullTimes.incrementAndGet();
                idleTimes.incrementAndGet();
            } else {
                idleTimes.set(0);
                busyTimes.incrementAndGet();
                pullTimes.incrementAndGet();
            }
        }

    }

    /**
     * 队列消费者
     */
    protected class QueueConsumer implements Runnable {
        // 暂存当前线程，用于连接断开时终止逻辑
        private Thread thread;
        // 是否关闭
        protected AtomicBoolean closed = new AtomicBoolean(false);
        // 统计数据
        protected PullStat pullStat = new PullStat();
        // 控制打印时间间隔
        protected final long PRINT_LOG_TIME_INTERVAL = 1000 * 60 * 10;


        public Thread getThread() {
            return thread;
        }

        public void setThread(Thread thread) {
            this.thread = thread;
        }

        /**
         * 时间切片
         *
         * @return 指定时间段里面的统计信息
         */
        public PullStat slice() {
            PullStat target = new PullStat(pullStat);
            pullStat.busyTimes.set(0);
            pullStat.pullTimes.set(0);
            return target;
        }

        /**
         * 是否关闭了
         *
         * @return 关闭标示
         */
        public boolean isClosed() {
            return closed.get();
        }

        public void close() {
            close(false);
        }

        public void close(boolean interrupt) {
            closed.set(true);
            if (interrupt) {
                // 终止当前线程
                if (this.thread != null) {
                    this.thread.interrupt();
                }
            }
        }


        @Override
        public void run() {
            long errors = 0;
            int count;
            TopicConfig topicConfig = transportManager.getClusterManager().getTopicConfig(topic);
            if (topicConfig != null && topicConfig.checkSequential()) {
                // 顺序消息暂停一会再去取消息！
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ignored) {
                }
            }

            config = transportManager.getClusterManager().getConsumerConfig(config, app, topic);
            int updatedPullEmptySleep = config.getPullEmptySleep();

            while (isStarted() && !isClosed()) {
                count = 0;
                try {
                    // 判断一下状态
                    if (!paused.get()) {
                        if (transport.getState() == FailoverState.CONNECTED) {
                            if (topicConfig != null && topicConfig.isLocalManageOffsetConsumer(app)) {
                                Short curQueue = queuePolling.poll();
                                if (curQueue != null) {
                                    try {
                                        count = pullBroadcastMessage(curQueue);
                                    } finally {
                                        queuePolling.add(curQueue);
                                    }
                                }
                            } else {
                                // 拉取消息
                                count = pull((short) 0, -1);
                            }
                        }
                        pullStat.update(count);
                    }
                    if (count <= 0) {
                        // 没有拉取到消息
                        sleep(updatedPullEmptySleep);
                    }
                    errors = 0;
                } catch (JMQException e) {
                    errors++;
                    if (e.getCode() == JMQCode.CN_NO_PERMISSION.getCode() || e
                            .getCode() == JMQCode.CN_SERVICE_NOT_AVAILABLE.getCode() || e
                            .getCode() == JMQCode.FW_CONNECTION_NOT_EXISTS.getCode() || e
                            .getCode() == JMQCode.FW_CONSUMER_NOT_EXISTS.getCode()) {
                        // 没有权限，服务不可用，连接和消费者信息丢失，需要重连
                        logger.info(e.getMessage());
                        transport.weak();
                    } else if (e.getCode() == JMQCode.CN_CONNECTION_ERROR.getCode()) {
                        // 连接异常
                        logger.info(e.getMessage());
                    } else if (e.getCode() == JMQCode.CN_REQUEST_TIMEOUT.getCode()) {
                        // 如果连接状态中，请求超时则输出日志。防止连接断开输出超时日志
                        if (transport.getState() == FailoverState.CONNECTED) {
                            // pull 请求超时
                            String solution = ExceptionHandler.getExceptionSolution(e.getMessage());
                            logger.info(e.getMessage() + solution, e);
                        }
                    } else if (e.getCode() == JMQCode.FW_GET_MESSAGE_APP_CLIENT_IP_NOT_READ.getCode() ||
                            e.getCode() == JMQCode.FW_GET_MESSAGE_TOPIC_NOT_READ.getCode()) {
                        boolean isNeedPrintLog = isNeedPrintLog(e.getCode());
                        if (isNeedPrintLog) {
                            logger.info(e.getMessage(), e);
                        }
                    } else {
                        logger.info(e.getMessage(), e);
                    }
                } catch (Throwable e) {
                    errors++;
                    logger.error(e.getMessage(), e);
                }
                // 出现了异常
                if (errors > 0) {
                    if (count <= 0) {
                        pullStat.update(0);
                    }
                    sleep(Math.min(updatedPullEmptySleep * errors, MAX_PULL_EMPTY_SLEEP_TIME));
                }
            }
            logger.info(String.format("a consumer is stopped,topic:%s,group:%s,queues:%s", topic, group.getGroup(), queues));
        }

        protected void sleep(long timeout) {
            try {
                Thread.sleep(timeout);
            } catch (InterruptedException ignored) {
            }
        }


        private int pullBroadcastMessage(short queueId) throws JMQException {
            int count = 0;
            long offset = localOffsetManage.getOffset(clientName, group.getGroup(), topic, queueId);
            long startTime = System.currentTimeMillis();
            count = pull(queueId, offset);
            long endTime = System.currentTimeMillis();
            if (logger.isDebugEnabled()) {
                logger.debug("队列号：" + queueId + "********" + "拉取时间：" + (endTime - startTime));
            }
            return count;
        }

        protected boolean isNeedPrintLog(int exceptionCode) {
            if (controlPrintLogMap == null || controlPrintLogMap.isEmpty()) {
                return true;
            }
            Long lastTimestamp = controlPrintLogMap.get(exceptionCode);
            if (lastTimestamp == null) {
                return true;
            }
            if (lastTimestamp == 0L) {
                controlPrintLogMap.put(exceptionCode, SystemClock.now());
                return true;
            }
            long currentTimestamp = SystemClock.now();
            if (currentTimestamp - lastTimestamp > PRINT_LOG_TIME_INTERVAL) {
                controlPrintLogMap.put(exceptionCode, SystemClock.now());
                return true;
            } else {
                return false;
            }
        }
    }

    /**
     * 消费者守护线程，定时运行
     */
    protected class ConsumerGuard implements Runnable {
        @Override
        public void run() {
            // 锁，防止在关闭，同时控制并发访问消费线程
            writeLock.lock();
            try {
                if (!isStarted()) {
                    return;
                }
                if (type != null && type == ConsumerType.PULL_DIRECT) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("topic %s, consumer %s type is PULL_DIRECT", topic, app));
                    }
                    return;
                }
                config = transportManager.getClusterManager().getConsumerConfig(config, app, topic);
                int maxConcurrent = config.getMaxConcurrent();
                int minConcurrent = config.getMinConcurrent();

                TopicConfig topicConfig = transportManager.getClusterManager().getTopicConfig(topic);
                if (topicConfig.checkSequential()) {
                    minConcurrent = 1;
                    maxConcurrent = 1;
                } else {
                    maxConcurrent = maxConcurrent > 0 ? maxConcurrent : queues;
                    minConcurrent = minConcurrent > 0 ? minConcurrent : 1;

                    if (minConcurrent > maxConcurrent) {
                        maxConcurrent = minConcurrent;
                    }
                }

                int actives = 0;
                // 记录空闲消费者
                List<QueueConsumer> idles = new ArrayList<QueueConsumer>();
                // 正常工作消费者
                List<QueueConsumer> workers = new ArrayList<QueueConsumer>();
                // 所有消费者忙
                boolean isAllBusy = true;
                PullStat pullStat;
                for (QueueConsumer consumer : consumers) {
                    // 拉取统计切片
                    pullStat = consumer.slice();
                    if (pullStat.isIdle()) {
                        idles.add(consumer);
                        isAllBusy = false;
                    } else {
                        workers.add(consumer);
                        if (!pullStat.isBusy()) {
                            isAllBusy = false;
                        }
                    }
                    actives++;
                }

                QueueConsumer consumer;
                if (actives > maxConcurrent) {
                    // 大于最大线程数，则减少线程
                    int count = actives - maxConcurrent;
                    // 优先减少空闲线程
                    while (count-- > 0) {
                        if (!idles.isEmpty()) {
                            consumer = idles.remove(0);
                        } else {
                            consumer = workers.remove(0);
                        }
                        consumer.close();
                        consumers.remove(consumer);
                        actives--;
                        logger.info(
                                String.format("remove a idle consumer,topic:%s,group:%s,queues:%d,maxConcurrent:%d,minConcurrent:%d,actives:%d",
                                        topic, group.getGroup(), queues, maxConcurrent, minConcurrent, actives));
                    }
                } else if (actives < minConcurrent || (maxConcurrent > 0 && isAllBusy && actives < maxConcurrent)) {
                    int count = 0;
                    if (actives < minConcurrent) {
                        count = minConcurrent - actives;
                    }

                    if ((maxConcurrent > 0 && isAllBusy && actives < maxConcurrent)) {
                        int temp = (maxConcurrent - actives) >= FAST_INC_THRESHOLD ? FAST_INC_COUNT : SLOW_INC_COUNT;
                        count = count > temp ? count : temp;
                    }

                    for (int i = 0; i < count; i++) {
                        consumer = new QueueConsumer();
                        consumers.add(consumer);
                        createThread(consumer);
                        actives++;
                        logger.info(String.format("add a consumer,topic:%s,group:%s,queues:%d,maxConcurrent:%d,minConcurrent:%d,actives:%d",
                                topic, group.getGroup(), queues, maxConcurrent, minConcurrent, actives));
                    }
                } else if (queues > 0 && !idles.isEmpty() && actives > minConcurrent) {
                    // 有空闲线程，并且当前线程数大于最小线程数，则慢慢递减消费线程
                    consumer = idles.get(0);
                    consumer.close();
                    consumers.remove(consumer);
                    actives--;
                    logger.info(String.format("remove a idle consumer,topic:%s,group:%s,queues:%d,maxConcurrent:%d,minConcurrent:%d,actives:%d",
                            topic, group.getGroup(), maxConcurrent, minConcurrent, queues, actives));
                }
            } finally {
                enqueue();
                writeLock.unlock();
            }

        }

        /**
         * 把改任务重新入队
         */
        protected void enqueue() {
            ScheduledExecutorService se = schedule;
            if (isStarted() && se != null) {
                try {
                    se.schedule(this, GUARD_INTERVAL, TimeUnit.MILLISECONDS);
                } catch (Exception ignored) {
                }
            }
        }
    }


    protected class FailoverStateListener implements EventListener<FailoverState> {
        @Override
        public void onEvent(FailoverState event) {
            if (event == null){
                return;
            }

            if (FailoverState.DISCONNECTED.equals(event) || FailoverState.WEAK.equals(event)) {
                TopicConfig config = transportManager.getClusterManager() != null ? transportManager.getClusterManager().getTopicConfig(topic) : null;
                if (config != null && config.checkSequential()) {
                    writeLock.lock();
                    try {
                        Iterator<QueueConsumer> it = consumers.iterator();
                        while (it.hasNext()) {
                            try {
                                QueueConsumer consumer = it.next();
                                consumer.close(true);
                                it.remove();
                            } catch (Exception e) {
                                logger.error("Close sequential queue consumer error!", e);
                            }
                        }
                    } finally {
                        writeLock.unlock();
                    }
                }
            }
        }
    }
}