package com.ipd.jmq.client.transaction;

import com.ipd.jmq.client.cluster.ClusterManager;
import com.ipd.jmq.client.connection.GroupTransport;
import com.ipd.jmq.client.connection.ProducerTransport;
import com.ipd.jmq.client.connection.TransportConfig;
import com.ipd.jmq.client.connection.TransportManager;
import com.ipd.jmq.common.model.ProducerConfig;
import com.ipd.jmq.common.cluster.BrokerGroup;
import com.ipd.jmq.common.cluster.Permission;
import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.network.v3.command.JMQHeader;
import com.ipd.jmq.common.network.v3.command.TxFeedback;
import com.ipd.jmq.common.network.v3.command.TxFeedbackAck;
import com.ipd.jmq.common.network.v3.command.TxStatus;
import com.ipd.jmq.common.network.v3.session.TransactionId;
import com.ipd.jmq.common.network.FailoverState;
import com.ipd.jmq.common.network.command.Command;
import com.ipd.jmq.toolkit.concurrent.NamedThreadFactory;
import com.ipd.jmq.toolkit.service.Service;
import com.ipd.jmq.toolkit.time.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * 事务补偿管理器.
 *
 * @author lindeqiang
 * @since 2016/5/26 17:41
 */
public class TxFeedbackManager extends Service {
    // 日志
    private static final Logger logger = LoggerFactory.getLogger(TxFeedbackManager.class);
    // 事务查询器(key为topic)
    private ConcurrentMap<String, TxStatusQuerier> queriers = new ConcurrentHashMap<String, TxStatusQuerier>();
    // 连接管理器
    private TransportManager transportManager;
    // 集群管理器
    private ClusterManager clusterManager;
    // 生产者配置信息
    private ProducerConfig config = new ProducerConfig();
    // 连接配置
    private TransportConfig transportConfig;
    // topicFeedback
    private Map<String, TopicFeedback> topicFeedback = new HashMap<String, TopicFeedback>();
    // executorService
    private ScheduledExecutorService executorService;

    public TxFeedbackManager() {
    }

    public TxFeedbackManager(TransportManager transportManager) {
        this.transportManager = transportManager;
    }

    public void setConfig(ProducerConfig config) {
        if (config != null) {
            this.config = config;
        }
    }

    public void setTransportManager(TransportManager transportManager) {
        this.transportManager = transportManager;
    }

    /**
     * 为了兼容spring,这里返回的是Map
     * 使用时应该转成ConcurrentHashMap
     * @return Map
     */
    public Map<String, TxStatusQuerier> getQueriers() {
        return queriers;
    }

    public void setQueriers(Map<String, TxStatusQuerier> queriers) {
        if (isStarted()) {
            return;
        }
        if (null == queriers || queriers.isEmpty()) {
            return;
        }
        for (String topic : queriers.keySet()) {
            this.queriers.put(topic, queriers.get(topic));
        }
    }

    /**
     * 添加查询器
     *
     * @param topic           topic
     * @param txStatusQuerier 查询器
     * @throws Exception topic为空或重复
     */
    public void addTxStatusQuerier(String topic, TxStatusQuerier txStatusQuerier) throws Exception {

        if (null == topic || topic.isEmpty()) {
            throw new IllegalArgumentException("Topic can not be null！");
        }

        if (txStatusQuerier == null) {
            throw new IllegalArgumentException("TxStatusQuerier can not be null！");
        }

        // 防止重复
        TxStatusQuerier old = queriers.putIfAbsent(topic, txStatusQuerier);
        if (old == null) {
            if (isStarted()) {
                try {
                    TopicFeedback feedback = new TopicFeedback(transportManager.getConfig().getApp(), topic);
                    feedback.start();
                    topicFeedback.put(topic, feedback);
                } catch (JMQException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        } else {
            throw new IllegalStateException(String.format("Corresponding txStatusQuerier already exists! topic=%s", topic));
        }
    }

    /**
     * 删除topic对应查询器
     *
     * @param topic topic
     * @throws Exception topic为空或不存在
     */
    public void removeTxStatusQuerier(String topic) throws Exception {
        if (null == topic || topic.isEmpty()) {
            throw new IllegalArgumentException("Topic can not be null！");
        }

        queriers.remove(topic);
        TopicFeedback toBeRemoved = topicFeedback.remove(topic);
        if (toBeRemoved != null) {
            toBeRemoved.stop();
        }
    }

    @Override
    protected void validate() throws Exception {
        super.validate();
        if (transportManager == null) {
            throw new IllegalStateException("transportManager can not be null");
        }
    }

    public void doStart() throws Exception {
        super.doStart();

        if (!transportManager.isStarted()) {
            transportManager.start();
        }

        if (clusterManager == null) {
            clusterManager = transportManager.getClusterManager();
        }

        if (transportConfig == null) {
            transportConfig = transportManager.getConfig();
        }

        if (config == null) {
            config = new ProducerConfig();
        }

        Set<String> topics = queriers.keySet();
        if (topics.size() > 0) {
            for (String topic : topics) {
                TopicFeedback feedback = new TopicFeedback(transportConfig.getApp(), topic);
                feedback.start();
                topicFeedback.put(topic, feedback);
            }

        }

        if (executorService == null) {
            // 任务调度(定时获取transports)
            executorService = Executors.newScheduledThreadPool(1, new NamedThreadFactory("GetTransportsSchedule"));
        }
        executorService.scheduleAtFixedRate(new GetTransportsScheduled(), 30000, 30000, TimeUnit.MILLISECONDS);
    }

    public void doStop() {
        super.doStop();

        // 清理全部线程
        if (topicFeedback != null && topicFeedback.size() > 0) {
            for (TopicFeedback feedback : topicFeedback.values()) {
                feedback.stop();
            }
        }
        if (null != executorService && !executorService.isShutdown()) {
            executorService.shutdownNow();
        }

        queriers.clear();
        topicFeedback.clear();
        queriers = null;
        topicFeedback = null;
    }


    private class GetTransportsScheduled implements Runnable {

        @Override
        public void run() {
            if (!isStarted()) {
                return;
            }

            if (queriers != null && !queriers.isEmpty()) {
                for (String topic : queriers.keySet()) {
                    // 获取连接
                    List<GroupTransport> transports = transportManager.getTransports(topic, Permission.WRITE);
                    TopicFeedback feedback = topicFeedback.get(topic);
                    feedback.updateTransport(transports);
                }
            }


        }
    }


    private class TopicFeedback extends Service {
        private Map<BrokerGroup, GroupFeedback> feedbackThreads = new HashMap<BrokerGroup, GroupFeedback>();
        private String app;
        private String topic;

        private TopicFeedback(final String app, final String topic) {
            this.app = app;
            this.topic = topic;
        }

        public void validate() throws Exception {
            if (topic == null || topic.isEmpty()) {
                throw new IllegalStateException("topic can not be null!");
            }

            if (app == null || app.isEmpty()) {
                throw new IllegalStateException("app can not be null!");
            }
        }

        @Override
        public void doStop() {
            super.doStop();
            for (GroupFeedback groupFeedback : feedbackThreads.values()) {
                groupFeedback.stop();
            }
            feedbackThreads.clear();
            feedbackThreads = null;
        }

        @Override
        public void doStart() throws Exception {
            super.doStart();
            checkState();
            // 获取连接
            List<GroupTransport> transports = transportManager.getTransports(topic, Permission.WRITE);

            if (transports != null && !transports.isEmpty()) {
                try {
                    for (GroupTransport groupTransport : transports) {
                        addGroupFeedback(groupTransport);
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            } else {
                // 没有可用连接
                logger.warn(String.format("主题 %s 没有可用连接", topic));
            }
        }

        public String getTopic() {
            return topic;
        }

        protected void updateTransport(List<GroupTransport> transports) {
            List<BrokerGroup> newGroups = new ArrayList<BrokerGroup>();
            for (GroupTransport transport : transports) {
                try {
                    newGroups.add(transport.getGroup());
                    // add
                    if (!feedbackThreads.containsKey(transport.getGroup())) {
                        addGroupFeedback(transport);
                    }
                } catch (Exception e) {
                    logger.error("Add group feedback failure!", e);
                }
            }
            Iterator<Map.Entry<BrokerGroup, GroupFeedback>> iterator = feedbackThreads.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<BrokerGroup, GroupFeedback> entry = iterator.next();
                BrokerGroup brokerGroup = entry.getKey();
                if (!newGroups.contains(brokerGroup)) {
                    GroupFeedback groupFeedback = entry.getValue();
                    if (null != groupFeedback) {
                        groupFeedback.stop();
                    }
                    iterator.remove();
                }
            }
        }

        private void addGroupFeedback(GroupTransport groupTransport) throws Exception {
            ProducerTransport transport = (ProducerTransport) groupTransport;
            GroupFeedback feedBack = new GroupFeedback(topic, app, transport);
            BrokerGroup group = groupTransport.getGroup();

            String name = "GroupFeedback_" + topic + "_" + group.getGroup() + SystemClock.now();
            Thread thread = new Thread(feedBack, name);
            feedBack.setThread(thread);
            feedBack.start();
            thread.start();

            feedbackThreads.put(group, feedBack);
        }

    }


    private class GroupFeedback extends Service implements Runnable {
        private String topic;
        private String app;
        private ProducerTransport transport;
        private Thread thread;

        public GroupFeedback(String topic, String app, ProducerTransport transport) {
            this.topic = topic;
            this.app = app;
            this.transport = transport;
        }

        private void setThread(Thread thread) {
            this.thread = thread;
        }


        @Override
        protected void validate() throws Exception {
            if (topic == null || topic.isEmpty()) {
                throw new IllegalStateException("topic can not be null");
            }
            if (app == null || app.isEmpty()) {
                throw new IllegalStateException("app can not be null");
            }
            if (transport == null) {
                throw new IllegalStateException("transport can not be null");
            }
            if (thread == null) {
                throw new IllegalStateException("thread can not be null");
            }
        }

        @Override
        protected void doStart() throws Exception {
            super.doStart();
        }

        @Override
        protected void doStop() {
            super.doStop();
            if (this.thread != null) {
                thread.interrupt();
            }
        }

        @Override
        public void run() {
            checkState();
            TxStatus txStatus = TxStatus.UNKNOWN;
            TransactionId transactionId = null;
            boolean feedBackSuccess = false;
            while (this.isStarted()) {
                //检查连接是否正常
                if (transport.getState() == FailoverState.CONNECTED) {
                    TxFeedback txFeedback = new TxFeedback();
                    try {
                        txFeedback.setProducerId(transport.getProducerId());
                        txFeedback.setApp(app);
                        txFeedback.setTopic(topic);

                        if (feedBackSuccess) {
                            txFeedback.setTxStatus(txStatus);
                            txFeedback.setTransactionId(transactionId);
                            feedBackSuccess = false;
                        }

                        Command request = new Command(JMQHeader.Builder.request(txFeedback.type(), config.getAcknowledge()), txFeedback);

                        Command response = transport.sync(request, transportConfig.getSendTimeout());
                        // 获取响应
                        JMQHeader header = (JMQHeader) response.getHeader();
                        // 成功
                        if (checkSuccess(header)) {
                            TxFeedbackAck ack = (TxFeedbackAck) response.getPayload();
                            transactionId = ack.getTransactionId();
                            // 没有事务ID,表示当前没有未提交的事务
                            if (null == transactionId) {
                                sleeping(100);
                            } else {
                                TxStatusQuerier querier = queriers.get(topic);
                                if (querier != null) {
                                    txStatus = querier.queryStatus(transactionId.getTransactionId(), ack.getQueryId());
                                    feedBackSuccess = true;
                                } else {
                                    logger.warn(String.format("corresponding querier is null! topic=%s", topic));
                                }
                            }
                        }
                    } catch (Exception e) {
                        logger.error(
                                String.format("feedback transaction(topic=%s, app=%s, transactionId=%s, " +
                                        "feedBackSuccess=%s) error", txFeedback.getTopic(), txFeedback.getApp(), transactionId, feedBackSuccess), e);
                    }
                } else {
                    logger.warn(String.format("Transport is abnormal.group=%s, state=%s", transport.getGroup(), transport.getState()));
                    sleeping(100);
                }

            }
        }

        private void sleeping(long time) {
            try {
                Thread.sleep(time);
            } catch (Exception ignored) {
            }
        }
    }


    /**
     * 状态验证
     */
    private void checkState() {
        if (!isStarted()) {
            throw new IllegalStateException("TxFeedbackManager haven't started. You can try to call the start() method.");
        }
    }

    /**
     * 判断返回值是否成功
     *
     * @param header    应答头
     * @return 请求是否成功
     */
    private boolean checkSuccess(final JMQHeader header) {
        int status = header.getStatus();
        return status == JMQCode.SUCCESS.getCode();
    }

}
