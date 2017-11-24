package com.ipd.jmq.server.broker;

import com.ipd.jmq.common.cluster.TopicConfig;
import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.network.v3.command.*;
import com.ipd.jmq.common.network.v3.session.Consumer;
import com.ipd.jmq.common.network.v3.session.Joint;
import com.ipd.jmq.server.broker.cluster.ClusterManager;
import com.ipd.jmq.server.broker.dispatch.DispatchService;
import com.ipd.jmq.server.broker.dispatch.PullResult;
import com.ipd.jmq.server.broker.monitor.BrokerMonitor;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.service.ServiceThread;
import com.ipd.jmq.toolkit.time.MilliPeriod;
import com.ipd.jmq.toolkit.time.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 长轮询管理器
 */
public class LongPullManager extends Service {

    protected static Logger logger = LoggerFactory.getLogger(LongPullManager.class);
    // 长轮询请求queue
    protected Queue<LongPull> longPulls = new LinkedBlockingQueue<LongPull>();
    // 消费者长轮询数量
    protected ConcurrentMap<Joint, AtomicInteger> counter = new ConcurrentHashMap<Joint, AtomicInteger>();
    // 消息获取。
    protected DispatchService dispatchService;
    // 会话管理器
    protected SessionManager sessionManager;
    // 异步处理。
    protected Thread guardThread = null;
    // 长轮询线程池
    protected ExecutorService executorService;
    // 集群管理器
    protected ClusterManager clusterManager;
    // 配置
    protected BrokerConfig config;
    // 监控管理
    protected BrokerMonitor brokerMonitor;
    public LongPullManager(){}
    public LongPullManager(ExecutorService executorService, SessionManager sessionManager,
                           ClusterManager clusterManager, DispatchService dispatchService, BrokerConfig config,
                           BrokerMonitor brokerMonitor) {

        if (executorService == null) {
            throw new IllegalArgumentException("executorService can not be null");
        }
        if (sessionManager == null) {
            throw new IllegalArgumentException("sessionManager can not be null");
        }
        if (clusterManager == null) {
            throw new IllegalArgumentException("clusterManager can not be null");
        }
        if (dispatchService == null) {
            throw new IllegalArgumentException("dispatchService can not be null");
        }
        if (config == null) {
            throw new IllegalArgumentException("config can not be null");
        }
        if (brokerMonitor == null) {
            throw new IllegalArgumentException("brokerMonitor can not be null");
        }
        this.executorService = executorService;
        this.sessionManager = sessionManager;
        this.clusterManager = clusterManager;
        this.config = config;
        this.dispatchService = dispatchService;
        this.brokerMonitor = brokerMonitor;
    }

    public void setDispatchService(DispatchService dispatchService) {
        this.dispatchService = dispatchService;
    }

    public void setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public void setClusterManager(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    public void setConfig(BrokerConfig config) {
        this.config = config;
    }

    public void setBrokerMonitor(BrokerMonitor brokerMonitor) {
        this.brokerMonitor = brokerMonitor;
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        counter.clear();
        // 守护进程，每100毫秒执行一次
        guardThread = new Thread(new ServiceThread(this, 100) {
            @Override
            public boolean onException(Throwable e) {
                logger.error(e.getMessage(), e);
                return true;
            }

            @Override
            protected void execute() throws Exception {
                // 处理长轮询
                processHoldRequest();
            }
        }, "LongPull");
        guardThread.start();
        logger.info("long pull manager is started");
    }

    @Override
    protected void doStop() {
        super.doStop();
        if (guardThread != null) {
            guardThread.interrupt();
        }
        counter.clear();
        logger.info("long pull manager is started");
    }

    /**
     * 获取消费者计数器
     *
     * @param consumer 消费者
     * @return 计数器
     */
    protected AtomicInteger getCount(Consumer consumer) {
        AtomicInteger count = counter.get(consumer.getJoint());
        if (count == null) {
            count = new AtomicInteger(0);
            AtomicInteger old = counter.putIfAbsent(consumer.getJoint(), count);
            if (old != null) {
                count = old;
            }
        }
        return count;
    }


    /**
     * 添加长轮询请求
     *
     * @param consumer  消费者
     * @param command   命令
     * @param transport 通道
     * @return 成功标示
     */
    public boolean suspend(Consumer consumer, Command command, Transport transport) {
        if (consumer == null || command == null || transport == null || !isStarted()) {
            return false;
        }

        // 主题信息
        TopicConfig topicConfig = clusterManager.getTopicConfig(consumer.getTopic());
        if (topicConfig == null) {
            return false;
        }
        // 长轮询数量不能超过主题队列数量
        AtomicInteger count = getCount(consumer);
        if (count.get() >= topicConfig.getQueues()) {
            return false;
        }
        // 超过最大容量
        int maxLongPulls = config.getMaxLongPulls();
        if (longPulls.size() > maxLongPulls) {
            return false;
        }

        // 入队
        if (longPulls.offer(new LongPull(consumer.getJoint(), command, transport))) {
            // 增加消费长轮询计数器
            count.incrementAndGet();
            return true;
        }
        return false;
    }

    /**
     * 处理长轮询请求，检查是否过期，是否有数据了
     */
    protected void processHoldRequest() throws Exception {
        long currentTime;
        int size = longPulls.size();
        LongPull longPull;
        GetMessage getMessage;
        Consumer consumer;
        AtomicInteger count;
        for (int i = 0; i < size; i++) {
            if (!isStarted()) {
                return;
            }
            currentTime = SystemClock.now();
            longPull = longPulls.poll();
            getMessage = longPull.getPayload();
            count = counter.get(longPull.getJoint());
            // 得到当前消费者
            consumer = sessionManager.getConsumerById(getMessage.getConsumerId().getConsumerId());
            if (consumer == null) {
                // 消费者不存在了，则抛弃该长轮询
                if (count != null) {
                    // 减少计数器
                    count.decrementAndGet();
                }
            } else if (longPull.getExpire() <= currentTime) {
                if (count != null) {
                    // 减少计数器
                    count.decrementAndGet();
                }
                // 长轮询过期了
                longPull.transport.acknowledge(longPull.getCommand(), createGetMessageAck(longPull.getCommand()), null);
            } else if (dispatchService.hasFreeQueue(consumer)) {
                // 有空闲队列
                executorService.execute(new GetMessageTask(consumer, longPull));
            } else {
                // 没有数据，则继续等待
                longPulls.offer(longPull);
            }
        }

    }

    protected Command createGetMessageAck(final Command command) {
        GetMessageAck ack = new GetMessageAck();
        return new Command(JMQHeader.Builder.create().
                direction(Direction.RESPONSE).
                requestId(command.getHeader().getRequestId()).
                type(ack.type()).build(), ack);

    }

    // 长轮询请求对象
    protected static class LongPull {
        private Command command;
        // 命令
        private GetMessage payload;
        // 通道
        private Transport transport;
        // 消费者
        private Joint joint;
        // 结束时间。
        private long expire;

        public LongPull(Joint joint, Command command, Transport transport) {
            this.joint = joint;
            this.command = command;
            this.payload = (GetMessage) command.getPayload();
            this.transport = transport;
            expire = SystemClock.now() + payload.getLongPull();
        }

        public Joint getJoint() {
            return joint;
        }

        public Command getCommand() {
            return command;
        }

        public GetMessage getPayload() {
            return payload;
        }

        public Transport getTransport() {
            return transport;
        }

        public long getExpire() {
            return expire;
        }
    }

    /**
     * 监听发送消息命令
     */
    protected static class LongPullListener implements CommandCallback {
        protected final PullResult pullResult;
        protected final GetMessageAck getMessageAck;

        public LongPullListener(PullResult pullResult, GetMessageAck getMessageAck) {
            this.pullResult = pullResult;
            this.getMessageAck = getMessageAck;
        }

        @Override
        public void onSuccess(Command command, Command command1) {
            // 确保释放资源
            getMessageAck.release();
        }

        @Override
        public void onException(Command command, Throwable throwable) {
            // 发送数据失败，清理队列锁
            pullResult.release(false, true);
            // 确保释放资源
            getMessageAck.release();
        }
    }

    /**
     * 重新拉取消息
     */
    protected class GetMessageTask implements Runnable {
        private final Command command;
        private final GetMessage getMessage;
        private final Consumer consumer;
        private final LongPull longPull;

        public GetMessageTask(Consumer consumer, LongPull longPull) {
            this.consumer = consumer;
            this.longPull = longPull;
            this.command = longPull.getCommand();
            this.getMessage = (GetMessage) command.getPayload();
        }

        @Override
        public void run() {
            if (!isStarted()) {
                return;
            }
            AtomicInteger count = counter.get(longPull.getJoint());
            final Command response = createGetMessageAck(command);
            final GetMessageAck getMessageAck = (GetMessageAck) response.getPayload();
            PullResult pullResult = null;
            try {
                // 获取消息
                MilliPeriod period = new MilliPeriod();
                period.begin();
                // 取数据
                pullResult = dispatchService.getMessage(consumer, getMessage.getCount(), getMessage.getAckTimeout(), getMessage.getQueueId(), getMessage.getOffset());
                if (pullResult != null && !pullResult.isEmpty()) {
                    // 监控信息
                    getMessageAck.setBuffers(pullResult.toArrays());
                    period.end();
                    brokerMonitor
                            .onGetMessage(consumer.getTopic(), consumer.getApp(), pullResult.count(), pullResult.size(),
                                    (int)period.time());
                    if (count != null) {
                        count.decrementAndGet();
                    }
                    // 应答，就算当前没有数据，也把资源释放了，让其它请求进入长轮询
                    longPull.transport.acknowledge(command, response, new LongPullListener(pullResult, getMessageAck));
                } else if (isStarted()) {
                    // 重新入队
                    longPulls.offer(longPull);
                }
            } catch (Throwable th) {
                try {
                    logger.error("long pull error.", th);
                    if (count != null) {
                        count.decrementAndGet();
                    }
                    // 释放资源
                    if (pullResult != null) {
                        pullResult.release(true, true);
                    }
                    // 错误消息头部
                    JMQHeader header = (JMQHeader) response.getHeader();
                    if (th instanceof JMQException) {
                        header.setStatus(((JMQException) th).getCode());
                    } else {
                        header.setStatus(JMQCode.CN_UNKNOWN_ERROR.getCode());
                    }
                    // 获取消息失败
                    header.setError(th.getMessage());
                    longPull.transport.acknowledge(command, response, null);
                } catch (Exception e) {
                    logger.error("ack long pull error.", e);
                }

            }
        }


    }
}