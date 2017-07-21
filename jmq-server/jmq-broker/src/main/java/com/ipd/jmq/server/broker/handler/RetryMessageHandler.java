package com.ipd.jmq.server.broker.handler;

import com.ipd.jmq.common.cluster.TopicConfig;
import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.BrokerMessage;
import com.ipd.jmq.common.message.MessageLocation;
import com.ipd.jmq.common.message.MessageQueue;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.v3.command.*;
import com.ipd.jmq.common.network.v3.netty.NettyDecoder;
import com.ipd.jmq.common.network.v3.session.Consumer;
import com.ipd.jmq.server.broker.BrokerConfig;
import com.ipd.jmq.server.broker.SessionManager;
import com.ipd.jmq.server.broker.cluster.ClusterManager;
import com.ipd.jmq.server.broker.dispatch.DispatchService;
import com.ipd.jmq.server.broker.monitor.BrokerMonitor;
import com.ipd.jmq.server.broker.retry.RetryManager;
import com.ipd.jmq.server.store.Store;
import com.ipd.jmq.toolkit.buffer.RByteBuffer;
import com.ipd.jmq.toolkit.concurrent.Scheduler;
import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.jmq.toolkit.time.SystemClock;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

//import com.ipd.jmq.server.broker.retry.CacheRetryManager;

/**
 * 重试消息处理器
 */
public class RetryMessageHandler extends AbstractHandler implements JMQHandler {
    protected static Logger logger = LoggerFactory.getLogger(GetMessageHandler.class);
    protected Store store;
    //共用的清理线程
    protected Scheduler cleanScheduler;
    protected BrokerConfig config;
    protected BrokerMonitor brokerMonitor;
    protected RetryManager retryManager;
    protected ClusterManager clusterManager;

    protected DispatchService dispatchService;

    //时间维度调用次数记数
    protected ConcurrentMap<String, ConcurrentMap<Long, AtomicInteger>> metricStat = new ConcurrentHashMap<String, ConcurrentMap<Long, AtomicInteger>>();
    protected long cacheWindowSize = 0;
    public RetryMessageHandler(){}
    public RetryMessageHandler(ExecutorService executorService, SessionManager sessionManager,
                               ClusterManager clusterManager, DispatchService dispatchService,
                               BrokerConfig config, Scheduler cleanScheduler, BrokerMonitor brokerMonitor) {

        Preconditions.checkArgument(config != null, "config can not be null");
        Preconditions.checkArgument(brokerMonitor != null, "brokerMonitor can not be null");
        Preconditions.checkArgument(cleanScheduler != null, "cleanScheduler can not be null");
        Preconditions.checkArgument(sessionManager != null, "sessionManager can not be null");
        Preconditions.checkArgument(clusterManager != null, "clusterManager can not be null");
        Preconditions.checkArgument(dispatchService != null, "dispatchService can not be null");
        Preconditions.checkArgument(executorService != null, "executorService can not be null");

        this.store = config.getStore();
        this.broker = clusterManager.getBroker();
        this.config = config;
        this.brokerMonitor = brokerMonitor;
        this.cleanScheduler = cleanScheduler;
        this.sessionManager = sessionManager;
        this.clusterManager = clusterManager;
        this.dispatchService = dispatchService;
        this.executorService = executorService;
        this.cacheWindowSize = config.getRetryConfig().getStatTimeWindowSize();
        cleanScheduler.scheduleWithFixedDelay(new CleanRunnable(), 10000, 5 * 60 * 1000);
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }
    public void setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }
    public void setCleanScheduler(Scheduler cleanScheduler) {
        this.cleanScheduler = cleanScheduler;
    }

    public void setConfig(BrokerConfig config) {
        this.config = config;
    }

    public void setBrokerMonitor(BrokerMonitor brokerMonitor) {
        this.brokerMonitor = brokerMonitor;
    }

    public void setRetryManager(RetryManager retryManager) {
        this.retryManager = retryManager;
    }

    public void setClusterManager(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    public void setDispatchService(DispatchService dispatchService) {
        this.dispatchService = dispatchService;
    }
    /**
     * 客户端重试
     *
     * @param transport
     * @param request
     * @param payload
     * @return
     * @throws JMQException
     */
    protected Command execute(final Transport transport, Command request, final RetryMessage payload) throws
            JMQException {
        Consumer consumer = sessionManager.getConsumerById(payload.getConsumerId().getConsumerId());
        if (consumer == null) {
            throw new JMQException(JMQCode.FW_CONSUMER_NOT_EXISTS);
        }
        TopicConfig.ConsumerPolicy consumerPolicy = null;
        TopicConfig topicConfig = clusterManager.getTopicConfig(consumer.getTopic());
        if (topicConfig != null) {
            consumerPolicy = topicConfig.getConsumerPolicy(consumer.getApp());
        }
        // 重试位置
        MessageLocation[] locations = payload.getLocations();
        Command reponse = null;
        if (locations != null && locations.length > 0) {
            // 重试错误的数据
            long[] retryIds = new long[locations.length];
            int retryCount = 0;
            List<RByteBuffer> messages = new ArrayList<RByteBuffer>(locations.length);
            List<MessageLocation> ackMessages = new ArrayList<MessageLocation>(locations.length);
            List<MessageLocation> retryMessages = new ArrayList<MessageLocation>(locations.length);
            try {
                // 遍历重试位置
                for (MessageLocation location : locations) {
                    if (location.getQueueId() == MessageQueue.RETRY_QUEUE) {
                        // 重试错误的数据
                        retryIds[retryCount++] = location.getQueueOffset();
                        retryMessages.add(location);
                    } else {
                        ackMessages.add(location);
                        RByteBuffer refBuf =
                                store.getMessage(location.getTopic(), location.getQueueId(), location.getQueueOffset());
                        if (refBuf != null) {
                            messages.add(refBuf);
                        } else {
                            logger.error("invalid location " + location);
                        }
                    }
                }

                // 重试错误,为重试队列中的错误，不需要再保存消息
                if (retryCount > 0) {
                    try {
                        retryManager.retryError(consumer.getTopic(), consumer.getApp(), retryIds);
                        //监控数据
                        brokerMonitor.onRetryError(consumer.getTopic(), consumer.getApp(), retryIds.length);
                    } catch (JMQException e) {
                        // 忽略错误
                        logger.error(e.getMessage(), e);
                    }

                    dispatchService.acknowledge(retryMessages.toArray(new MessageLocation[retryMessages.size()]), consumer, false);
                }
                if (!ackMessages.isEmpty()) {
                    //增加用户异常记录，应答成功会清理掉
                    dispatchService.addConsumeErr(consumer.getId(), false);
                    // 启用了重试，正常消息转重试
                    if ((consumerPolicy == null || (consumerPolicy.isRetry() && !consumerPolicy.checkSequential()))) {
                        boolean isLimit = isNeedLimit(consumer.getTopic(), consumer.getApp(), SystemClock.now(), cacheWindowSize, config.getRetryConfig().getLimitTimes());
                        if (isLimit) {
                            // 让消费可以继续消费
                            dispatchService.cleanExpire(consumer, ackMessages.get(0).getQueueId());
                            reponse = CommandUtils.createBooleanAck(request.getHeader().getRequestId(), JMQCode
                                    .CT_LIMIT_REQUEST);
                            ((JMQHeader) reponse.getHeader()).setError(String.format("重试调用指定时间%d(ms)内超过限制次数%d", config
                                    .getRetryConfig().getStatTimeWindowSize(), config.getRetryConfig().getLimitTimes()));
                        } else {
                            try {
                                if (!messages.isEmpty()) {
                                    BrokerMessage[] msgArr = new BrokerMessage[messages.size()];
                                    String err = dealMessageInfo(messages, msgArr, payload.getException(), consumer
                                            .getTopic(), consumer.getApp());
                                    // 添加到重试数据库
                                    retryManager.addRetry(consumer.getTopic(), consumer.getApp(), msgArr, err);
                                    //监控
                                    brokerMonitor.onAddRetry(consumer.getTopic(), consumer.getApp(), msgArr.length);
                                }
                                // 应答
                                dispatchService.acknowledge(ackMessages.toArray(new MessageLocation[ackMessages.size()]),
                                        consumer, true);
                            } catch (Exception e) {
                                // 让消费可以继续消费
                                dispatchService.cleanExpire(consumer, ackMessages.get(0).getQueueId());
                                //todo:并行消费需要把对应的block加入到过期里面供其他消费者消费
                                throw e;
                            }
                        }

                    } else {
                        // 没用启用重试，则清理过期时间，可以立即继续消费
                        dispatchService.cleanExpire(consumer, ackMessages.get(0).getQueueId());
                        //todo:并行消费需要把对应的block加入到过期里面供其他消费者消费
                    }
                }
            } catch (JMQException e) {
                throw e;
            } catch (Exception e) {
                throw new JMQException(e, JMQCode.CN_UNKNOWN_ERROR.getCode());
            } finally {
                if (!messages.isEmpty()) {
                    for (RByteBuffer buf : messages) {
                        if (buf != null) {
                            buf.release();
                        }
                    }
                }
            }
        }
        return reponse == null ? CommandUtils.createBooleanAck(request.getHeader().getRequestId(), JMQCode.SUCCESS) : reponse;
    }

    protected boolean isNeedLimit(String topic, String app, long time, long cacheWindowSize, int limitTimes) {
        //有线程可能正在清理，判断会有失误，但是不加锁进行处理了
        ConcurrentMap<Long, AtomicInteger> stat = new ConcurrentHashMap<Long, AtomicInteger>();
        ConcurrentMap<Long, AtomicInteger> oldStat = metricStat.putIfAbsent(topic + "@" + app, stat);
        if (oldStat != null) {
            stat = oldStat;
        }
        Long statWind = getStatKey(time, cacheWindowSize);
        AtomicInteger times = new AtomicInteger(0);
        AtomicInteger oldTimes = stat.putIfAbsent(statWind, times);
        if (oldTimes != null) {
            times = oldTimes;
        } else {
            //清除不用数据KEY
            Iterator<Map.Entry<Long, AtomicInteger>> iterator = stat.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Long, AtomicInteger> statEntry = iterator.next();
                if (statEntry.getKey() < statWind) {
                    iterator.remove();
                }
            }
        }
        int curTimes = times.incrementAndGet();
        return curTimes > limitTimes;
    }

    private String dealMessageInfo(List<RByteBuffer> messages, BrokerMessage[] msgArr, String err, String topic, String app)
            throws Exception {
        int msgLen = 0;
        for (int i = 0; i < messages.size(); i++) {
            msgLen += messages.get(i).remaining();
            msgArr[i] = Serializer
                    .readBrokerMessage(Unpooled.wrappedBuffer(messages.get(i).getBuffer()));
        }
        if (err == null) {
            err = "";
        }
        int errLen = err.getBytes("UTF-8").length;
        msgLen += errLen;
        int maxFrameSize = clusterManager.getConfig().getServerConfig().getFrameMaxSize() - 1024;
        if (msgLen >= maxFrameSize) {
            logger.warn(String.format("topic:%s,app:%s,msg too length:%d,errLen:%d,stack:%s", topic, app, msgLen, errLen, err));
            int remainLen = (maxFrameSize - (msgLen - errLen));
            if (err.length() > 0 && errLen == err.length()) {
                err = err.substring(0, err.length() > remainLen ? remainLen : err.length());
            } else if (err.length() > 0 && errLen > err.length() && remainLen / 3 > 0) {
                err = err.substring(0, err.length() > remainLen / 3 ? remainLen / 3 : err.length());
            }
        }
        return err;
    }

    /**
     * 服务端远程调用，新增重试
     *
     * @param transport
     * @param request
     * @param command
     * @return
     * @throws JMQException
     */
    protected Command execute(final Transport transport, final Command request, final PutRetry command) throws
            JMQException {
        retryManager.addRetry(command.getTopic(), command.getApp(), command.getMessages(), command.getException());
        return CommandUtils.createBooleanAck(request.getHeader().getRequestId(), JMQCode.SUCCESS);
    }

    /**
     * 由DBRetryManager所在Broker调用
     *
     * @param transport
     * @param request
     * @param command
     * @return
     * @throws JMQException
     */
    protected Command execute(final Transport transport, Command request, final GetRetry command) throws JMQException {

        RetryManager retryManager = this.retryManager;
//        if (retryManager instanceof CacheRetryManager) {
//            retryManager = ((CacheRetryManager) retryManager).getDelegate();
//        }
//
//        if (retryManager == null) {
//            throw new JMQException(JMQCode.CN_SERVICE_NOT_AVAILABLE);
//        }

        List<BrokerMessage> messageList =
                retryManager.getRetry(command.getTopic(), command.getApp(), command.getCount(), command.getStartId());

        if (messageList.size() > 1) {
            //限制返回的重试消息的长度
            int maxFrameSize = NettyDecoder.FRAME_MAX_SIZE - 1024;
            int dataSize = 0;
            int index = 0;
            for (BrokerMessage message : messageList) {
                dataSize += message.getByteBody().length;
                if (dataSize < maxFrameSize) {
                    index++;
                } else {
                    break;
                }
            }
            messageList = messageList.subList(0, index);
        }

        BrokerMessage[] messages = messageList.toArray(new BrokerMessage[messageList.size()]);

        GetRetryAck ack = new GetRetryAck();
        ack.messages(messages);

        return new Command(JMQHeader.Builder.create().type(ack.type()).direction(Direction.RESPONSE).status(JMQCode.SUCCESS
                .getCode()).error(JMQCode.SUCCESS.getMessage()).build(), ack);

    }

    /**
     * 获取重试数据条数
     *
     * @param transport
     * @param request
     * @param command
     * @return
     * @throws JMQException
     */
    protected Command execute(final Transport transport, final Command request, final GetRetryCount command) throws
            JMQException {

        RetryManager retryManager = this.retryManager;
        if (retryManager == null) {
            throw new JMQException(JMQCode.CN_SERVICE_NOT_AVAILABLE);
        }

        int count = retryManager.getRetry(command.getTopic(), command.getApp());

        GetRetryCountAck ack = new GetRetryCountAck();
        ack.topic(command.getTopic()).app(command.getApp()).count(count);
        return new Command(JMQHeader.Builder.create().type(ack.type()).direction(Direction.RESPONSE).
                requestId(request.getHeader().getRequestId()).status(JMQCode.SUCCESS.getCode()).
                error(JMQCode.SUCCESS.getMessage()).build(), ack);
    }

    /**
     * 服务端远程调用，修改重试
     *
     * @param transport
     * @param request
     * @param command
     * @return
     * @throws JMQException
     */
    protected Command execute(final Transport transport, final Command request, final UpdateRetry command) throws
            JMQException {
        int type = command.getUpdateType();

        if (UpdateRetry.SUCCESS == type) {
            retryManager.retrySuccess(command.getTopic(), command.getApp(), command.getMessages());
        } else if (UpdateRetry.EXPIRED == type) {
            retryManager.retryExpire(command.getTopic(), command.getApp(), command.getMessages());
        } else {
            retryManager.retryError(command.getTopic(), command.getApp(), command.getMessages());
        }

        return CommandUtils.createBooleanAck(request.getHeader().getRequestId(), JMQCode.SUCCESS);
    }


    @Override
    public int[] type() {
        return new int[]{
                CmdTypes.PUT_RETRY,
                CmdTypes.GET_RETRY,
                CmdTypes.UPDATE_RETRY,
                CmdTypes.RETRY_MESSAGE,
                CmdTypes.GET_RETRY_COUNT};
    }

    @Override
    public Command process(Transport transport, Command command) throws TransportException {
        try {
            JMQHeader header = (JMQHeader) command.getHeader();

            switch (header.getType()) {
                case CmdTypes.PUT_RETRY:
                    return execute(transport, command, (PutRetry) command.getPayload());
                case CmdTypes.GET_RETRY:
                    return execute(transport, command, (GetRetry) command.getPayload());
                case CmdTypes.UPDATE_RETRY:
                    return execute(transport, command, (UpdateRetry) command.getPayload());
                case CmdTypes.RETRY_MESSAGE:
                    return execute(transport, command, (RetryMessage) command.getPayload());
                case CmdTypes.GET_RETRY_COUNT:
                    return execute(transport, command, (GetRetryCount) command.getPayload());
                default:
                    throw new JMQException(JMQCode.CN_COMMAND_UNSUPPORTED.getMessage(header.getType()),
                            JMQCode.CN_COMMAND_UNSUPPORTED.getCode());
            }

        } catch (JMQException e) {
            throw new TransportException(e.getMessage(), e.getCause(), e.getCode());
        }
    }

    public class CleanRunnable implements Runnable {
        public void run() {
            if (logger.isDebugEnabled()) {
                logger.debug("start clean metricStat");
            }
            try {
                //维度改变全部清理
                if (cacheWindowSize != config.getRetryConfig().getStatTimeWindowSize()) {
                    metricStat.clear();
                    cacheWindowSize = config.getRetryConfig().getStatTimeWindowSize();
                    return;
                }

                long curStatKey = getStatKey(SystemClock.now(), cacheWindowSize);
                List<String> emptyKey = new LinkedList<String>();
                for (Map.Entry<String, ConcurrentMap<Long, AtomicInteger>> entry : metricStat.entrySet()) {
                    Iterator<Map.Entry<Long, AtomicInteger>> iterator = entry.getValue().entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<Long, AtomicInteger> statEntry = iterator.next();
                        if (statEntry.getKey() < curStatKey) {
                            iterator.remove();
                        }
                    }
                    if (entry.getValue().isEmpty()) {
                        emptyKey.add(entry.getKey());
                    }
                }
                //可能误删，但是不影响逻辑，不加锁
                for (String key : emptyKey) {
                    metricStat.remove(key);
                }
            } catch (Exception e) {
                logger.error("", e);
            }
        }
    }

    private Long getStatKey(long now, long windSize) {
        return now / windSize;
    }

}