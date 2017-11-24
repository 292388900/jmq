package com.ipd.jmq.server.broker;

import com.ipd.jmq.common.cluster.TopicConfig;
import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.*;
import com.ipd.jmq.common.network.v3.command.*;
import com.ipd.jmq.common.network.v3.session.Producer;
import com.ipd.jmq.common.network.v3.session.ProducerId;
import com.ipd.jmq.common.network.v3.session.TransactionId;
import com.ipd.jmq.server.broker.archive.ArchiveManager;
import com.ipd.jmq.server.broker.cluster.ClusterManager;
import com.ipd.jmq.server.broker.monitor.BrokerMonitor;
import com.ipd.jmq.server.store.GetTxResult;
import com.ipd.jmq.server.store.Store;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.server.store.StoreContext;
import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.service.ServiceThread;
import com.ipd.jmq.toolkit.time.MilliPeriod;
import com.ipd.jmq.toolkit.time.Period;
import com.ipd.jmq.toolkit.time.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 分布式事务管理器
 * Created by dingjun on 16-5-5.
 */
public class TxTransactionManager extends Service {
    protected static Logger logger = LoggerFactory.getLogger(TxTransactionManager.class);
    protected BrokerMonitor brokerMonitor;
    protected ArchiveManager archiveManager;
    protected ClusterManager clusterManager;
    private ExecutorService executorService;
    private SessionManager sessionManager;
    private TxQueryManager queryManager;
    private Store store;
    public TxTransactionManager(){}
    public TxTransactionManager(BrokerConfig config){
        store = config.getStore();
    }
    public TxTransactionManager(ClusterManager clusterManager, BrokerConfig config,
                                BrokerMonitor brokerMonitor,
                                ExecutorService executorService, SessionManager sessionManager) {

        Preconditions.checkArgument(executorService != null, "executorService can not be null");
        Preconditions.checkArgument(sessionManager != null, "sessionManager can not be null");
        Preconditions.checkArgument(clusterManager != null, "clusterManager can not be null");
        Preconditions.checkArgument(config != null, "config can not be null");
        Preconditions.checkArgument(brokerMonitor != null, "brokerMonitor can not be null");

        store = config.getStore();

        this.brokerMonitor = brokerMonitor;
        this.clusterManager = clusterManager;
        this.sessionManager = sessionManager;
        this.executorService = executorService;
    }

    public void setArchiveManager(ArchiveManager archiveManager) {
        this.archiveManager = archiveManager;
    }

    public void setBrokerMonitor(BrokerMonitor brokerMonitor) {
        this.brokerMonitor = brokerMonitor;
    }

    public void setClusterManager(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public void setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }


    public void doStart() throws Exception {
        queryManager = new TxQueryManager();
        queryManager.start();
    }

    public void doStop() {
        queryManager.stop();
    }


    /**
     * 检查状态
     *
     * @throws JMQException
     */
    protected void checkState() throws JMQException {
        if (!isStarted()) {
            throw new JMQException(JMQCode.CN_SERVICE_NOT_AVAILABLE);
        }
    }

    /**
     * 准备事务
     *
     * @param prepare
     * @return
     * @throws JMQException
     */
    public void TxPrepare(final TxPrepare prepare) throws JMQException {
        checkState();
        BrokerPrepare innerMsg = new BrokerPrepare();
        innerMsg.setAttrs(prepare.getAttrs());
        innerMsg.setQueryId(prepare.getQueryId());
        innerMsg.setTopic(prepare.getTopic());
        innerMsg.setTxId(prepare.getTransactionId().getTransactionId());
        innerMsg.setStartTime(prepare.getStartTime());
        innerMsg.setTimeout(prepare.getTimeout());
        innerMsg.setTimeoutAction(prepare.getTimeoutAction());
        // TODO 处理回调
        store.putJournalLog(innerMsg);
    }

    /**
     * 提交事务
     *
     * @param commit
     * @return 复制状态
     * @throws JMQException
     */
    public void TxCommit(final TxCommit commit, String app) throws JMQException {
        checkState();
        try {
            MilliPeriod period = new MilliPeriod();
            period.begin();
            TransactionId transactionId = commit.getTransactionId();
            BrokerPrepare brokerPrepare = store.getBrokerPrepare(transactionId.getTransactionId());
            BrokerCommit innerMsg = new BrokerCommit();
            innerMsg.setTopic(commit.getTopic());
            innerMsg.setTxId(transactionId.getTransactionId());
            // TODO 处理回调
            store.putJournalLog(innerMsg);
            onPutMessage(brokerPrepare, app, period);
        } catch (Exception e) {
            brokerMonitor.onSendFailure();
            logger.error("commit transaction error.", e);
            throw new JMQException(e.getMessage(), JMQCode.CN_TRANSACTION_COMMIT_ERROR.getCode());
        }
    }

    private void onPutMessage(final BrokerPrepare prepare, String app, Period period) {
        if (prepare == null) {
            return;
        }

        List<BrokerRefMessage> refMessages = prepare.getMessages();
        int count = refMessages != null ? refMessages.size() : 0;
        int size = 0;

        if (count > 0) {
            for (BrokerRefMessage refMessage : refMessages) {
                size += refMessage.getRefJournalSize();
            }
            period.end();
            brokerMonitor.onPutMessage(prepare.getTopic(), app, count, size, (int) period.time());
        }
    }


    /**
     * 回滚事务
     *
     * @param rollback
     * @return 复制状态
     * @throws JMQException
     */
    public void TxRollback(final TxRollback rollback) throws JMQException {
        checkState();
        BrokerRollback innerMsg = new BrokerRollback();
        innerMsg.setTopic(rollback.getTopic());
        innerMsg.setTxId(rollback.getTransactionId().getTransactionId());
        // TODO 处理回调
        store.putJournalLog(innerMsg);
    }

    public void addTxMessage(final List<BrokerMessage> messages) throws JMQException {
        checkState();
        if (messages == null || messages.isEmpty()) {
            return ;
        }
        // TODO 处理回调
        String topic = messages.get(0).getTopic();
        TopicConfig config = clusterManager.getTopicConfig(topic);
        final boolean isArchive = config.isArchive();
        StoreContext storeContext = new StoreContext(topic, messages.get(0).getApp(), messages, new Store.ProcessedCallback() {
            @Override
            public void onProcessed(StoreContext context) {
                //TODO 归档
                if (isArchive && context.getResult().getCode() == JMQCode.SUCCESS) { //归档
                    for (BrokerMessage message : messages) {
                        archiveManager.writeProduce(message);
                    }
                }
            }
        });
        store.putJournalLogs(storeContext);
    }


    public void txFeedBack(TxFeedback query) throws JMQException {
        checkState();
        TransactionId transactionId = query.getTransactionId();

        if (transactionId != null) {
            try {
                store.onTransactionFeedback(transactionId.getTransactionId());
                TxStatus status = query.getTxStatus();
                if (status == TxStatus.COMMITTED) {
                    MilliPeriod period = new MilliPeriod();
                    period.begin();
                    BrokerCommit innerMsg = new BrokerCommit();
                    innerMsg.setTopic(query.getTopic());
                    innerMsg.setTxId(transactionId.getTransactionId());
                    BrokerPrepare brokerPrepare = store.getBrokerPrepare(innerMsg.getTxId());
                    // TODO 处理回调
                    store.putJournalLog(innerMsg);

                    onPutMessage(brokerPrepare, query.getApp(), period);
                }
                if (status == TxStatus.ROLLBACK) {
                    BrokerRollback innerMsg = new BrokerRollback();
                    innerMsg.setTopic(query.getTopic());
                    innerMsg.setTxId(transactionId.getTransactionId());
                    // TODO 处理回调
                    store.putJournalLog(innerMsg);
                }
            } catch (Exception e) {
                logger.error(String.format("process transaction feed back error!topic=%s,txId=%s,txStatus=%s", query
                        .getTopic(), transactionId, query.getTxStatus()), e);

                // 解锁当前锁住的事务
                store.unLockInflightTransaction(transactionId.getTransactionId());
            }
        }

    }


    public GetTxResult getInflightTransaction(TxFeedback query) throws JMQException {
        checkState();
        return store.getInflightTransaction(query.getTopic(), query.getProducerId().getProducerId());
    }


    public boolean addQueryToLongPull(Command query, Transport transport) {
        return queryManager.append(query, transport);
    }


    protected class TxQueryManager extends Service {
        private Thread queryThread;
        // 长轮询请求queue
        protected Queue<QueryRequest> queries = new LinkedBlockingQueue<QueryRequest>();
        protected ConcurrentMap<String, AtomicInteger> counter = new ConcurrentHashMap<String, AtomicInteger>();

        public void doStart() throws Exception {
            super.doStart();
            counter.clear();
            queryThread = new Thread(new ServiceThread(this, 100) {
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
            }, "JMQ_SERVER_TX_QUERY_THREAD");
            queryThread.start();
            logger.info("transaction query manager is started");
        }

        public void doStop() {
            super.doStop();
            if (queryThread != null) {
                queryThread.interrupt();
            }
            counter.clear();
            queries.clear();
            logger.info("transaction query manager is stopped");
        }

        protected void processHoldRequest() throws Exception {

            int queryCount = queries.size();

            for (int i = 0; i < queryCount; i++) {
                long currentTime = SystemClock.now();

                QueryRequest request = queries.poll();
                AtomicInteger count = getCount(request.getTopic());

                Producer producer = sessionManager.getProducerById(request.getProducerId().getProducerId());
                if (producer == null) {
                    if (count != null) {
                        count.decrementAndGet();
                    }
                } else if (request.getExpire() <= currentTime) {
                    if (count != null) {
                        count.decrementAndGet();
                    }
                    Transport transport = request.getTransport();
                    if (transport != null) {
                        transport.acknowledge(request.getQuery(), createQueryAck(request.getQuery()), null);
                    }
                } else {
                    executorService.submit(new QueryTask(request));
                }
            }
        }

        public boolean append(Command query, Transport transport) {
            if (query == null || transport == null) {
                return false;
            }
            TxFeedback payload = (TxFeedback) query.getPayload();

            AtomicInteger count = getCount(payload.getTopic());

            if (count.get() < 1) {
                QueryRequest request = new QueryRequest();
                request.setTransport(transport);
                request.setQuery(query);
                request.setExpire(SystemClock.now() + payload.getLongPull());

                if (queries.offer(request)) {
                    count.incrementAndGet();
                }
                return true;
            }

            return false;
        }

        protected AtomicInteger getCount(String topic) {
            AtomicInteger count = counter.get(topic);
            if (count == null) {
                count = new AtomicInteger(0);
                AtomicInteger old = counter.putIfAbsent(topic, count);
                if (old != null) {
                    count = old;
                }
            }
            return count;
        }

        protected class QueryTask implements Runnable {
            private QueryRequest request;

            public QueryTask(QueryRequest request) {
                this.request = request;
            }

            @Override
            public void run() {
                AtomicInteger count = getCount(request.getTopic());
                Command queryAck = createQueryAck(request.getQuery());
                final TxFeedbackAck payload = (TxFeedbackAck) queryAck.getPayload();
                Transport transport = request.getTransport();
                GetTxResult getResult = null;
                try {
                    getResult = store.getInflightTransaction(request.getTopic(), request.getProducerId().getProducerId());
                    if (getResult != null) {
                        payload.setQueryId(getResult.getQueryId());
                        payload.setTopic(getResult.getTopic());
                        payload.setTxStatus(TxStatus.PREPARE);
                        payload.setTxStartTime(getResult.getStartTime());
                        payload.setTransactionId(new TransactionId(getResult.getTxId()));
                        if (count != null) {
                            count.decrementAndGet();
                        }
                        if (transport != null) {
                            transport.acknowledge(request.getQuery(), queryAck, new TxQueryListener(getResult, store));
                        }
                    } else if (isStarted()) {
                        queries.offer(request);
                    }
                } catch (Throwable th) {
                    try {
                        logger.error("query transaction error!", th);
                        if (count != null) {
                            count.decrementAndGet();
                        }
                        if (getResult != null) {
                            store.unLockInflightTransaction(getResult.getTxId());
                        }
                        // 错误消息头部
                        JMQHeader header = (JMQHeader) queryAck.getHeader();

                        if (th instanceof JMQException) {
                            header.setStatus(((JMQException) th).getCode());
                        } else {
                            header.setStatus(JMQCode.CN_UNKNOWN_ERROR.getCode());
                        }
                        // 获取消息失败
                        header.setError(th.getMessage());

                        transport.acknowledge(request.getQuery(), queryAck, null);
                    } catch (Exception e) {
                        logger.error("query transaction error!", th);
                    }

                }
            }
        }


        protected class QueryRequest {
            private Command query;
            private TxFeedback payload;
            private Transport transport;
            private long expire;

            public String getTopic() {
                return payload.getTopic();
            }

            public ProducerId getProducerId() {
                return payload.getProducerId();
            }

            public Transport getTransport() {
                return transport;
            }

            public void setTransport(Transport transport) {
                this.transport = transport;
            }

            public long getExpire() {
                return expire;
            }

            public void setExpire(long expire) {
                this.expire = expire;
            }

            public String getApp() {
                return payload.getApp();
            }

            public Command getQuery() {
                return query;
            }

            public void setQuery(Command query) {
                this.query = query;
                this.payload = (TxFeedback) query.getPayload();
            }

            public TxFeedback getPayload() {
                return payload;
            }
        }

    }

    protected Command createQueryAck(Command query) {
        TxFeedbackAck ack = new TxFeedbackAck();
        return new Command(JMQHeader.Builder.create().
                direction(Direction.RESPONSE).
                requestId(query.getHeader().getRequestId()).
                type(ack.type()).build(), ack);
    }


    /**
     * 监听发送消息命令
     */
    protected static class TxQueryListener implements CommandCallback {
        protected final GetTxResult getResult;
        protected final Store store;

        public TxQueryListener(GetTxResult getResult, Store store) {
            this.getResult = getResult;
            this.store = store;
        }

        @Override
        public void onSuccess(Command command, Command command1) {
            // do nothing
        }

        @Override
        public void onException(Command command, Throwable throwable) {
            // 发送数据失败，解锁当前事务
            store.unLockInflightTransaction(getResult.getTxId());
        }
    }
}
