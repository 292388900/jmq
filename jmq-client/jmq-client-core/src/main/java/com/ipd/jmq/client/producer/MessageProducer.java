package com.ipd.jmq.client.producer;

import com.ipd.jmq.client.cluster.ClusterManager;
import com.ipd.jmq.client.connection.GroupTransport;
import com.ipd.jmq.client.connection.ProducerTransport;
import com.ipd.jmq.client.connection.TransportConfig;
import com.ipd.jmq.client.connection.TransportManager;
import com.ipd.jmq.client.exception.ExceptionHandler;
import com.ipd.jmq.client.stat.Trace;
import com.ipd.jmq.client.stat.TraceInfo;
import com.ipd.jmq.client.stat.TracePhase;
import com.ipd.jmq.client.transaction.TxFeedbackManager;
import com.ipd.jmq.common.cluster.Permission;
import com.ipd.jmq.common.cluster.TopicConfig;
import com.ipd.jmq.common.model.ProducerConfig;
import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.Message;
import com.ipd.jmq.common.network.v3.command.*;
import com.ipd.jmq.common.network.v3.session.TransactionId;
import com.ipd.jmq.common.network.FailoverState;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.model.Acknowledge;
import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.time.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 消息生产者
 */
public class MessageProducer extends Service implements Producer {
    // 日志
    private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);
    // 生产者配置信息
    protected ProducerConfig config = new ProducerConfig();
    // 负载均衡
    protected LoadBalance loadBalance;
    // 连接通道管理器
    protected TransportManager transportManager;
    // 应用
    protected TransportConfig transportConfig;
    // 需要进行健康检查的异常
    protected Set<Integer> weakCodes = new HashSet<Integer>();
    // 集群管理器
    protected ClusterManager clusterManager;
    // 消息发送异常的时间
    protected ConcurrentMap<String, Long> sendErrorTimes = new ConcurrentHashMap<String, Long>();
    // 性能跟踪
    protected Trace trace;
    // 记录控制打印日志异常的时间戳
    protected ConcurrentMap<Integer, Long> controlPrintLogMap = new ConcurrentHashMap<Integer, Long>();
    // 控制打印时间间隔
    protected final long PRINT_LOG_TIME_INTERVAL = 1000 * 60 * 10;
    //开启的事务ID
    private ThreadLocal<TxPrepare> txPrepare = new ThreadLocal<TxPrepare>();
    //事务保证同一连接
    private ThreadLocal<ProducerTransport> txTransport = new ThreadLocal<ProducerTransport>();
    // 事务补偿管理器
    private TxFeedbackManager feedbackManager;



    public MessageProducer() {
        this(null);
    }

    public MessageProducer(TransportManager transportManager) {
        this.transportManager = transportManager;
        this.weakCodes.add(JMQCode.SE_CREATE_FILE_ERROR.getCode());
        this.weakCodes.add(JMQCode.SE_FLUSH_TIMEOUT.getCode());
        this.weakCodes.add(JMQCode.SE_DISK_FULL.getCode());
        this.weakCodes.add(JMQCode.SE_IO_ERROR.getCode());
        this.weakCodes.add(JMQCode.SE_FATAL_ERROR.getCode());
        this.weakCodes.add(JMQCode.CN_SERVICE_NOT_AVAILABLE.getCode());
        this.weakCodes.add(JMQCode.CN_NO_PERMISSION.getCode());
        this.weakCodes.add(JMQCode.FW_CONNECTION_NOT_EXISTS.getCode());
        this.weakCodes.add(JMQCode.FW_PRODUCER_NOT_EXISTS.getCode());
        this.controlPrintLogMap.put(JMQCode.FW_PUT_MESSAGE_TOPIC_NOT_WRITE.getCode(), 0L);
        this.controlPrintLogMap.put(JMQCode.FW_PUT_MESSAGE_APP_CLIENT_IP_NOT_WRITE.getCode(), 0L);
    }

    public void setTxFeedbackManager(TxFeedbackManager feedbackManager) {
        if (null == feedbackManager) {
            return;
        }
        this.feedbackManager = feedbackManager;
    }

    public ProducerConfig getConfig(){
        return config;
    }

    @Override
    protected void validate() throws Exception {
        super.validate();
        if (transportManager == null) {
            throw new IllegalStateException("transportManager can not be null");
        }
        if (!transportManager.isStarted()) {
            throw new IllegalStateException("transportManager is not started");
        }
        if (clusterManager == null) {
            clusterManager = transportManager.getClusterManager();
        }
        if (config == null) {
            config = new ProducerConfig();
        }
        if (loadBalance == null) {
            loadBalance = new WeightLoadBalance();
        }
        if (trace == null) {
            trace = transportManager.getTrace();
        }
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        if (!transportManager.isStarted()) {
            transportManager.start();
        }
        if (feedbackManager != null && !feedbackManager.isStarted()) {
            feedbackManager.start();
        }
        transportConfig = transportManager.getConfig();

        logger.info("message producer is started");
    }

    @Override
    protected void doStop() {
        super.doStop();
        sendErrorTimes.clear();
        if (feedbackManager != null && !feedbackManager.isStopped()) {
            feedbackManager.stop();
        }
        logger.info("message producer is stopped");
    }

    public void setConfig(ProducerConfig config) {
        this.config = config;
    }

    public void setLoadBalance(LoadBalance loadBalance) {
        this.loadBalance = loadBalance;
    }

    public void setTransportManager(TransportManager transportManager) {
        this.transportManager = transportManager;
    }

    /**
     * 状态验证
     */
    protected void checkState() {
        if (!isStarted()) {
            throw new IllegalStateException("Producer haven't started. You can try to call the start() method.");
        }
    }

    @Override
    public void send(final Message message) throws JMQException {
        checkState();
        if (message == null) {
            return;
        }
        List<Message> messages = new ArrayList<Message>();
        messages.add(message);
        send(messages);
    }

    @Override
    public void send(final List<Message> messages) throws JMQException {
        checkState();
        send(messages, false, null);
    }

    @Override
    public void sendAsync(final Message message, final AsyncSendCallback callback) throws JMQException {
        checkState();
        if (message == null) {
            return;
        }
        List<Message> messages = new ArrayList<Message>();
        messages.add(message);
        sendAsync(messages, callback);
    }

    @Override
    public void sendAsync(final List<Message> messages, final AsyncSendCallback callback) throws JMQException {
        checkState();
        send(messages, true, callback);
    }

    private void send(final List<Message> messages, final boolean isAsync, final AsyncSendCallback callback) throws JMQException {
        if (messages == null || messages.isEmpty()) {
            return;
        }

        boolean isTxMsg = this.txPrepare.get() != null
                && this.txPrepare.get().getTransactionId() != null
                && this.txTransport.get() != null;
        try {
            TransactionId txId = null;
            if (isTxMsg) {
                txId = checkAndGetTxId();
                if (this.txPrepare.get().getMessages() != null) {
                    txPrepare.get().setMessages(messages);
                }
            }

            long startTime = SystemClock.now();
            // 主题
            Message message = messages.get(0);
            String topic = message.getTopic();
            TopicConfig topicConfig = clusterManager.getTopicConfig(topic);
            long size = checkMessages(messages, topicConfig, startTime, isTxMsg);
            int count = messages.size();

            // 性能统计
            TraceInfo info = new TraceInfo(topic, transportConfig.getApp(), TracePhase.PRODUCE, startTime);
            trace.begin(info);

            int localSendTimeout = config.getSendTimeout() > 0 ? config.getSendTimeout() : transportConfig.getSendTimeout();
            config.setSendTimeout(localSendTimeout);
            // 计算重试次数和
            config = clusterManager.getProducerConfig(config, transportConfig.getApp(), topic);
            int retryTimes = config.getRetryTimes();
            // acknowledge
            Acknowledge acknowledge = config.getAcknowledge();
            // 计算超时时间
            long sendTimeout = config.getSendTimeout();
            // 剩余时间
            long remainTime;

            List<GroupTransport> transports = null;
            ProducerTransport transport = null;
            if (isTxMsg) {
                transport = this.txTransport.get();
                if (null == transport || transport.getState() != FailoverState.CONNECTED) {
                    // 没用可用连接，更新性能统计
                    error(info, topicConfig, count, new JMQException(JMQCode.CT_NO_CLUSTER, topic));
                }
            } else {
                // 获取生产连接
                transports = transportManager.getTransports(topic, Permission.WRITE);
                if (transports == null || transports.isEmpty()) {
                    // 没用可用连接，更新性能统计
                    error(info, topicConfig, count, new JMQException(JMQCode.CT_NO_CLUSTER, topic));
                }

                retryTimes = Math.min(retryTimes, transports == null ? 0 : transports.size() - 1);
            }

            if (!isTxMsg) {
                // 判断是否有连接可用，如果都没有连接上，则等待一段时间
                try {
                    checkConnection(transports, sendTimeout);
                } catch (JMQException e) {
                    // 没用可用连接，更新性能统计
                    error(info, topicConfig, count, e);
                }
            }

            JMQException error = null;
            List<GroupTransport> errTransports = new ArrayList<GroupTransport>();
            // 发送，出错重试
            for (int i = 0; i < retryTimes + 1; i++) {
                checkState();
                // 计算剩余超时时间
                remainTime = sendTimeout - (SystemClock.now() - startTime);
                if (remainTime <= 0) {
                    // 超时
                    error(info, topicConfig, count, new JMQException(JMQCode.CN_REQUEST_TIMEOUT, "发送失败"));
                }
                // 构造数据包发送
                try {
                    // 选择连接
                    if (!isTxMsg) {
                        transport = (ProducerTransport) loadBalance.electTransport(transports, errTransports, message,
                                clusterManager.getDataCenter(), topicConfig);
                    }

                    PutMessage putMessage = new PutMessage();
                    putMessage.producerId(transport.getProducerId())
                            .messages(messages.toArray(new Message[messages.size()]));
                    putMessage.queueId(loadBalance.electQueueId(transport, message, clusterManager.getDataCenter(), topicConfig));
                    if (isTxMsg) {
                        putMessage.transactionId(txId);
                    }

                    Command request = new Command(JMQHeader.Builder.request(putMessage.type(), acknowledge), putMessage);
                    Command response;

                    try {
                        if (!isAsync) {
                            response = transport.sync(request, (int) remainTime);
                        } else {
                            transport.async(request, (int) remainTime, callback);
                            response = CommandUtils.createBooleanAck(request.getHeader().getRequestId(), JMQCode.SUCCESS);
                        }
                    } catch (TransportException e) {
                        throw new JMQException(e.getMessage(), e.getCode());
                    }

                    JMQHeader header = (JMQHeader) response.getHeader();
                    // 判断服务不健康
                    if (!checkSuccess(header, transport)) {
                        throw new JMQException(header.getError(), header.getStatus());
                    }
                    if (logger.isTraceEnabled()) {
                        logger.trace(
                                String.format("send message to %s,size:%d,time:%d", transport.getGroup().getGroup(), size,
                                        SystemClock.now() - startTime));
                    }
                    // 发送成功，更新性能统计
                    info.count(count).size(size).success();
                    trace.end(info);
                    return;
                } catch (JMQException e) {
                    error = e;
                    if (!isTxMsg) {
                        // 出错把当前链接移除
                        errTransports.add(transport);//transports.remove(transport);
                    }
                    if (i <= retryTimes) {
                        if (e.getCode() == JMQCode.FW_PUT_MESSAGE_APP_CLIENT_IP_NOT_WRITE.getCode() ||
                                e.getCode() == JMQCode.FW_PUT_MESSAGE_TOPIC_NOT_WRITE.getCode()) {
                            boolean isNeedPrintLog = isNeedPrintLog(e.getCode());
                            if (isNeedPrintLog) {
                                logger.error(
                                        String.format("fail sending message(topic=%s,businessId=%s) to %s,retry %d. error:%s ",
                                                topic, message.getBusinessId(), transport.getGroup().getGroup(), i + 1, e.getMessage()));
                            }
                        } else {
                            logger.error(
                                    String.format("fail sending message(topic=%s,businessId=%s) to %s,retry %d. error:%s ",
                                            topic, message.getBusinessId(), transport.getGroup().getGroup(), i + 1, e.getMessage()));
                        }

                    }
                }
            }
            // 出错，更新性能统计
            error(info, topicConfig, count, error);
        } catch (Throwable e) {
            if (isTxMsg) {
                // 清理开启的事务
                this.txPrepare.remove();
                this.txTransport.remove();
            }

            if (e instanceof JMQException) {
                throw (JMQException) e;
            }

            throw new JMQException(JMQCode.CN_UNKNOWN_ERROR, e);
        }
    }


    @Override
    public String beginTransaction(final String topic, final String queryId, final int txTimeout) throws JMQException {
        //发送 TxPrepare 类型消息
        //返回 PutRichMessageAck
        //开启事务成功后将返回的事务ID 存放于 this.transaction

        checkState();
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("Topic can not be null!");
        }

        TxPrepare prepare = this.txPrepare.get();
        // 还有未完成的事务
        if (prepare != null) {
            throw new IllegalStateException("Exist unfinished transaction. transactionId=" + prepare.getTransactionId());
        }

        long startTime = SystemClock.now();

        // 消息条数
        int count = 1;
        // 消息大小
        long size = 0;

        TopicConfig topicConfig = clusterManager.getTopicConfig(topic);

        // 性能统计
        TraceInfo info = new TraceInfo(topic, transportConfig.getApp(), TracePhase.PRODUCE, startTime);
        trace.begin(info);

        // 获取连接
        List<GroupTransport> transports = transportManager.getTransports(topic, Permission.WRITE);
        if (transports == null || transports.isEmpty()) {
            // 没用可用连接，更新性能统计
            error(info, topicConfig, count, new JMQException(JMQCode.CT_NO_CLUSTER, topic));
        }

        int localSendTimeout = config.getSendTimeout() > 0 ? config.getSendTimeout() : transportConfig.getSendTimeout();
        config.setSendTimeout(localSendTimeout);
        // 计算重试次数和
        config = clusterManager.getProducerConfig(config, transportConfig.getApp(), topic);
        int retryTimes = config.getRetryTimes();
        retryTimes = Math.min(retryTimes, transports == null ? 0 : transports.size() - 1);
        // 计算超时时间
        long sendTimeout = config.getSendTimeout();
        // 剩余超时时间
        long remainTime;

        // 判断是否有连接可用，如果都没有连接上，则等待一段时间
        try {
            checkConnection(transports, sendTimeout);
        } catch (JMQException e) {
            // 没用可用连接，更新性能统计
            error(info, topicConfig, count, e);
        }

        ProducerTransport transport;
        TxPrepareAck ack;
        JMQHeader header;
        boolean prepareSuccess = false;
        JMQException error = null;
        List<GroupTransport> errTransports = new ArrayList<GroupTransport>();
        // 重试事务准备阶段
        for (int i = 0; i < retryTimes + 1; i++) {
            checkState();
            // 剩余的时间
            remainTime = sendTimeout - (SystemClock.now() - startTime);
            // 判断超时
            if (remainTime <= 0) {
                // 超时
                error(info, topicConfig, count, new JMQException(JMQCode.CN_REQUEST_TIMEOUT, "开启事务失败"));
            }
            // 选举连接
            transport = (ProducerTransport) loadBalance.electTransport(transports, errTransports, null, clusterManager.getDataCenter(), topicConfig);
            try {
                // 发送准备事务命令
                TxPrepare txPrepare = new TxPrepare();
                txPrepare.setProducerId(transport.getProducerId());
                txPrepare.setQueryId(queryId);
                txPrepare.setStartTime(startTime);
                txPrepare.setTimeout(txTimeout);
                //txPrepare.setMessages(messages);
                txPrepare.setTopic(topic);

                Command request = new Command(JMQHeader.Builder.request(txPrepare.type(), config.getAcknowledge()), txPrepare);

                Command response;
                try {
                    response = transport.sync(request, (int) remainTime);
                } catch (TransportException e) {
                    throw new JMQException(e.getMessage(), e.getCause(), e.getCode());
                }

                // 获取响应
                header = (JMQHeader) response.getHeader();
                if (!checkSuccess(header, transport)) {
                    throw new JMQException(header.getError(), JMQCode.CN_TRANSACTION_PREPARE_ERROR.getCode());
                }
                ack = (TxPrepareAck) response.getPayload();
                prepareSuccess = true;

                //保存连接
                this.txTransport.set(transport);
                //设置事务ID并保存事务
                txPrepare.setTransactionId(ack.getTransactionId());
                this.txPrepare.set(txPrepare);
                // 准备成功，退出循环
                break;
            } catch (JMQException e) {
                error = e;
                // 出错，移除该连接
                errTransports.add(transport);//transports.remove(transport);
                if (i <= retryTimes) {
                    logger.error(
                            String.format("fail preparing transaction(topic=%s) to %s,retry %d. " +
                                            "error:%s ",
                                    topic, transport.getGroup().getGroup(), i + 1, e.getMessage()));
                }
            }
        }
        if (!prepareSuccess) {
            // 最大重试次数则退出
            error(info, topicConfig, count,
                    new JMQException(error == null ? "error is null" : error.getMessage(), JMQCode.CN_TRANSACTION_PREPARE_ERROR.getCode()));
        }

        // 发送成功，更新性能统计
        info.count(count).size(size).success();
        trace.end(info);

        // 返回事务ID
        return this.txPrepare.get().getTransactionId().getTransactionId();
    }

    //@Override
    private void sendTxMessages(List<Message> messages) throws JMQException {
        //发送 PutRichMessage
        //发送前从 this.transaction 中获取事务ID
        //返回 PutRichMessageAck
        try {
            checkState();
            if (messages == null || messages.isEmpty()) {
                return;
            }

            TransactionId txId = checkAndGetTxId();
            long startTime = SystemClock.now();
            // 主题
            Message message = messages.get(0);
            String topic = message.getTopic();
            TopicConfig topicConfig = clusterManager.getTopicConfig(topic);
            long size = checkMessages(messages, topicConfig, startTime, true);
            int count = messages.size();
            TxPrepare localTxPrepare = this.txPrepare.get();
            if (null != localTxPrepare && null == localTxPrepare.getMessages()) {
                localTxPrepare.setMessages(messages);
            }

            // 性能统计
            TraceInfo info = new TraceInfo(topic, transportConfig.getApp(), TracePhase.PRODUCE, startTime);
            trace.begin(info);


            int localSendTimeout = config.getSendTimeout() > 0 ? config.getSendTimeout() : transportConfig.getSendTimeout();
            config.setSendTimeout(localSendTimeout);
            // 计算重试次数和
            config = clusterManager.getProducerConfig(config, transportConfig.getApp(), topic);
            int retryTimes = config.getRetryTimes();
            // 计算超时时间
            long sendTimeout = config.getSendTimeout();
            // 剩余时间
            long remainTime;

            JMQException error = null;
            ProducerTransport transport = this.txTransport.get();

            if (null == transport || transport.getState() != FailoverState.CONNECTED) {
                // 没用可用连接，更新性能统计
                error(info, topicConfig, count, new JMQException(JMQCode.CT_NO_CLUSTER, topic));
            }
            // 构造数据包
            PutRichMessage putRichMessage = new PutRichMessage();
            putRichMessage.transactionId(txId).producerId(transport.getProducerId())
                    .messages(messages.toArray(new Message[messages.size()]));
            putRichMessage.queueId(loadBalance.electQueueId(transport, message, clusterManager.getDataCenter(), topicConfig));

            // 发送，出错重试
            for (int i = 0; i < retryTimes + 1; i++) {
                checkState();
                // 计算剩余超时时间
                remainTime = sendTimeout - (SystemClock.now() - startTime);
                if (remainTime <= 0) {
                    // 超时
                    error(info, topicConfig, count, new JMQException(JMQCode.CN_REQUEST_TIMEOUT, "发送事务消息失败"));
                }
                try {
                    //发送数据包
                    Command request = new Command(JMQHeader.Builder.request(putRichMessage.type(), config.getAcknowledge()), putRichMessage);
                    Command response = transport.sync(request, (int) remainTime);
                    JMQHeader header = (JMQHeader) response.getHeader();
                    // 判断服务不健康
                    if (!checkSuccess(header, transport)) {
                        throw new JMQException(header.getError(), header.getStatus());
                    }
                    if (logger.isTraceEnabled()) {
                        logger.trace(
                                String.format("send message to %s,size:%d,time:%d", transport.getGroup().getGroup(), size,
                                        SystemClock.now() - startTime));
                    }

                    // 发送成功，更新性能统计
                    info.count(count).size(size).success();
                    trace.end(info);
                    return;
                } catch (JMQException e) {
                    error = e;
                    if (i <= retryTimes) {
                        logger.error(
                                String.format("fail sending message(topic=%s,businessId=%s) to %s,retry %d. error:%s ",
                                        topic, message.getBusinessId(), transport.getGroup().getGroup(), i + 1, e.getMessage()));
                    }
                }
            }
            // 出错，更新性能统计
            error(info, topicConfig, count, error);
        } catch (Throwable e) {
            // 清理开启的事务
            this.txPrepare.remove();
            this.txTransport.remove();

            if (e instanceof JMQException) {
                throw (JMQException) e;
            }

            throw new JMQException(JMQCode.CN_UNKNOWN_ERROR, e);
        }

    }

    @Override
    public void rollbackTransaction() throws JMQException {
        //发送 TxRollback
        //返回 PutRichMessageAck
        //回滚后将 this.transaction 清空
        checkState();
        try {
            TransactionId txId = checkAndGetTxId();

            List<Message> messages = this.txPrepare.get().getMessages();
            long startTime = SystemClock.now();

            // 主题
            String topic = this.txPrepare.get().getTopic();
            TopicConfig topicConfig = clusterManager.getTopicConfig(topic);
            long size = (null == messages || messages.isEmpty()) ? 0 : checkMessages(messages, topicConfig, startTime, true);

            // 性能统计
            TraceInfo info = new TraceInfo(topic, transportConfig.getApp(), TracePhase.PRODUCE, startTime);
            trace.begin(info);
            int count = (null == messages || messages.isEmpty()) ? 1 : messages.size();

            int localSendTimeout = config.getSendTimeout() > 0 ? config.getSendTimeout() : transportConfig.getSendTimeout();
            config.setSendTimeout(localSendTimeout);
            // 计算重试次数和
            config = clusterManager.getProducerConfig(config, transportConfig.getApp(), topic);
            int retryTimes = config.getRetryTimes();
            // 计算超时时间
            long sendTimeout = config.getSendTimeout();
            // 剩余超时时间
            long remainTime;

            ProducerTransport transport = this.txTransport.get();
            if (null == transport || transport.getState() != FailoverState.CONNECTED) {
                error(info, topicConfig, count, new JMQException(JMQCode.CT_NO_CLUSTER, topic));
            }

            Command response;
            JMQHeader header;
            boolean rollbackSuccess = false;
            JMQException error = null;

            // 构建回滚命令
            TxRollback txRollback = new TxRollback();
            txRollback.setTopic(topic);
            txRollback.setTransactionId(txId);

            // 重试事务 rollback 阶段
            for (int i = 0; i < retryTimes + 1; i++) {
                checkState();
                // 剩余的时间
                remainTime = sendTimeout - (SystemClock.now() - startTime);
                // 判断超时
                if (remainTime <= 0) {
                    // 超时
                    error(info, topicConfig, count, new JMQException(JMQCode.CN_REQUEST_TIMEOUT, "回滚失败"));
                }
                try {
                    // 发送回滚命令
                    Command request = new Command(JMQHeader.Builder.request(txRollback.type(), config.getAcknowledge()), txRollback);

                    try {
                        response = transport.sync(request, (int) remainTime);
                    } catch (TransportException e) {
                        throw new JMQException(e.getMessage(), e.getCause(), e.getCode());
                    }

                    // 获取响应
                    header = (JMQHeader) response.getHeader();
                    if (!checkSuccess(header, transport)) {
                        throw new JMQException(header.getError(), JMQCode.CN_TRANSACTION_ROLLBACK_ERROR.getCode());
                    }
                    rollbackSuccess = true;
                    break;
                } catch (JMQException e) {
                    error = e;
                    // 出错，移除该连接
                    if (i <= retryTimes) {
                        logger.error(
                                String.format("fail rollback transaction(topic=%s,transactionId=%s) to %s,retry %d. " +
                                                "error:%s ",
                                        topic, txId.getTransactionId(), transport.getGroup().getGroup(), i + 1, e.getMessage()));
                    }
                }
            }
            if (!rollbackSuccess) {
                // 最大重试次数则退出
                error(info, topicConfig, count, new JMQException(error.getMessage(), JMQCode.CN_TRANSACTION_PREPARE_ERROR.getCode()));
            }

            // 发送成功，更新性能统计
            info.count(count).size(size).success();
            trace.end(info);
        } finally {
            //回滚后将 this.transaction 清空
            this.txPrepare.remove();
            this.txTransport.remove();
        }
    }

    @Override
    public void commitTransaction() throws JMQException {
        //发送 TxCommit
        //返回 PutRichMessageAck
        //提交后将 this.transaction 清空

        checkState();
        try {
            TransactionId txId = checkAndGetTxId();
            List<Message> messages = this.txPrepare.get().getMessages();
            long startTime = SystemClock.now();

            // 主题
            String topic = this.txPrepare.get().getTopic();
            TopicConfig topicConfig = clusterManager.getTopicConfig(topic);
            long size = (null == messages || messages.size() == 0) ? 0 : checkMessages(messages, topicConfig, startTime, true);

            // 性能统计
            TraceInfo info = new TraceInfo(topic, transportConfig.getApp(), TracePhase.PRODUCE, startTime);
            trace.begin(info);
            int count = (null == messages || messages.size() == 0) ? 1 : messages.size();

            int localSendTimeout = config.getSendTimeout() > 0 ? config.getSendTimeout() : transportConfig.getSendTimeout();
            config.setSendTimeout(localSendTimeout);
            // 计算重试次数和
            config = clusterManager.getProducerConfig(config, transportConfig.getApp(), topic);
            int retryTimes = config.getRetryTimes();
            // 计算超时时间
            long sendTimeout = config.getSendTimeout();
            // 剩余超时时间
            long remainTime;

            ProducerTransport transport = this.txTransport.get();
            if (null == transport || transport.getState() != FailoverState.CONNECTED) {
                // 没用可用连接，更新性能统计
                error(info, topicConfig, count, new JMQException(JMQCode.CT_NO_CLUSTER, topic));
            }

            Command response;
            JMQHeader header;
            boolean commitSuccess = false;
            JMQException error = null;
            // 重试事务提交阶段
            // 构建提交命令
            TxCommit txCommit = new TxCommit();
            txCommit.setTopic(topic);
            txCommit.setTransactionId(txId);
            for (int i = 0; i < retryTimes + 1; i++) {
                checkState();
                // 剩余的时间
                remainTime = sendTimeout - (SystemClock.now() - startTime);
                // 判断超时
                if (remainTime <= 0) {
                    // 超时
                    error(info, topicConfig, count, new JMQException(JMQCode.CN_REQUEST_TIMEOUT, "事务提交失败"));
                }
                try {
                    // 发送提交命令
                    Command request = new Command(JMQHeader.Builder.request(txCommit.type(), config.getAcknowledge()), txCommit);

                    try {
                        response = transport.sync(request, (int) remainTime);
                    } catch (TransportException e) {
                        throw new JMQException(e.getMessage(), e.getCause(), e.getCode());
                    }

                    // 获取响应
                    header = (JMQHeader) response.getHeader();
                    if (!checkSuccess(header, transport)) {
                        throw new JMQException(header.getError(), JMQCode.CN_TRANSACTION_PREPARE_ERROR.getCode());
                    }
                    commitSuccess = true;
                    break;
                } catch (JMQException e) {
                    error = e;
                    if (i <= retryTimes) {
                        logger.error(
                                String.format("fail commit transaction(topic=%s,transactionId=%s) to %s,retry %d. " +
                                                "error:%s ",
                                        topic, txId.getTransactionId(), transport.getGroup().getGroup(), i + 1, e.getMessage()));
                    }
                }
            }
            if (!commitSuccess) {
                // 最大重试次数则退出
                error(info, topicConfig, count, new JMQException(error.getMessage(), JMQCode.CN_TRANSACTION_PREPARE_ERROR.getCode()));
            }

            // 发送成功，更新性能统计
            info.count(count).size(size).success();
            trace.end(info);
        } finally {
            //提交后将 this.transaction 清空
            this.txPrepare.remove();
            this.txTransport.remove();
        }
    }

    private void updateErrorTimeAndRequestCluster(String topic) {
        long current = SystemClock.now();
        Long pre = sendErrorTimes.get(topic);
        if (pre == null || pre == 0) {
            pre = current;
            Long old = sendErrorTimes.putIfAbsent(topic, current);
            if (old != null) {
                pre = old;
            }
        }
        // 发送报错时间设置为当前时间
        sendErrorTimes.put(topic, current);
        int interval = clusterManager.getUpdateClusterInterval();
        boolean updateNow = current - pre > interval;
        clusterManager.updateCluster(updateNow);
    }


    /**
     * 出现异常
     *
     * @param info  统计
     * @param config 主题配置
     * @param count 条数
     * @param e     异常
     * @throws JMQException
     */
    protected void error(final TraceInfo info, TopicConfig config, final int count, final JMQException e) throws JMQException {
        info.count(count).error(e);
        trace.end(info);
        if (null == config) {
            config = clusterManager.getTopicConfig(info.getTopic());
        }
        // 顺序消息出错设置发送错误次数，请求更新集群信息
        if (config != null && config.checkSequential()) {
            updateErrorTimeAndRequestCluster(info.getTopic());
        }

        // 主题XXX没有可用集群/连接
        if (e.getCode() == JMQCode.CT_NO_CLUSTER.getCode()) {
            String exceptionMessage = JMQCode.CT_NO_CLUSTER.getMessage();
            String solution = ExceptionHandler.getExceptionSolution(exceptionMessage);
            // 异常会再抛出，因此这里就不打异常栈
            logger.error(e.getMessage() + solution);
        }
        throw e;
    }

    /**
     * 检查连接是否可用
     *
     * @param transports  连接
     * @param sendTimeout 发送超时时间
     * @throws JMQException
     */
    protected void checkConnection(final List<GroupTransport> transports, final long sendTimeout) throws JMQException {
        for (GroupTransport transport : transports) {
            if (transport.getState() == FailoverState.CONNECTED) {
                return;
            }
        }
        // 每次sleep时间和循环检测次数
        long sleepTime = 100;
        int count = (int) (sendTimeout / sleepTime);
        if (count < 5) {
            count = 10;
            sleepTime = sendTimeout / count;
            if (sleepTime < 10) {
                sleepTime = 10;
            }
        }
        for (int i = 0; i < count; i++) {
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                throw new JMQException(JMQCode.CN_REQUEST_TIMEOUT, "没有可用连接");
            }
            for (GroupTransport transport : transports) {
                if (transport.getState() == FailoverState.CONNECTED) {
                    return;
                }
            }
        }
        throw new JMQException(JMQCode.CN_REQUEST_TIMEOUT, "没有可用连接");
    }

    /**
     * 检查消息合法性
     *
     * @param messages 消息列表
     * @param config   主题配置
     * @param sendTime 发送时间
     * @param txMsg    是否是事务消息
     * @return 消息的总大小
     */
    protected int checkMessages(final List<Message> messages, TopicConfig config, final long sendTime, boolean txMsg) {
        int size = 0;
        String topic = null;
        for (Message message : messages) {
            message.setSendTime(sendTime);
            message.setApp(transportConfig.getApp());
            if (message.getTopic() == null || message.getTopic().isEmpty()) {
                throw new IllegalArgumentException("message.topic can not be empty");
            }
            if (topic == null) {
                topic = message.getTopic();
            } else if (!topic.equals(message.getTopic())) {
                throw new IllegalArgumentException("message.topic must be the same");
            }

            if (config == null) {
                config = clusterManager.getTopicConfig(topic);
            }

            if (config != null && config.checkSequential()) {
                // 顺序消息默认设置为顺序
                message.ordered(true);
            }

            if (message.getBusinessId() != null && message.getBusinessId().length() > 100) {
                logger.warn(String.format("businessId:%s is too long, it must less than 100 characters!!", message
                        .getBusinessId()));
            }

            size += message.getSize();

        }

        if (txMsg) {
            TxPrepare prepare = txPrepare.get();
            if (prepare != null && topic != null && !topic.equals(prepare.getTopic())) {
                throw new IllegalArgumentException("message.topic and prepare.topic must be the same");
            }
        }

        if (size > clusterManager.getMaxSize()) {
            throw new IllegalArgumentException(
                    "the total bytes of message body must be less than " + clusterManager.getMaxSize());
        }

        return size;
    }

    /**
     * 判断返回值是否成功
     *
     * @param header    应答头
     * @param transport 传输通道
     * @return 请求是否成功
     */
    protected boolean checkSuccess(final JMQHeader header, final GroupTransport transport) {
        int status = header.getStatus();
        if (status == JMQCode.SUCCESS.getCode()) {
            return true;
        }

        if (weakCodes.contains(status)) {
            if (transport.getState() != FailoverState.CONNECTED) {
                logger.error(
                        String.format("channel is weak, %s  %s", header.getError(), transport.getGroup().toString()));
            }
            //启动健康检查
            transport.weak();
        }

        return false;
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


    private TransactionId checkAndGetTxId() {
        TransactionId txId = this.txPrepare.get() == null ? null : this.txPrepare.get().getTransactionId();
        if (txId == null) {
            // 未开启事务
            throw new IllegalStateException("Not exists opening transaction.");
        }

        return txId;
    }

}


