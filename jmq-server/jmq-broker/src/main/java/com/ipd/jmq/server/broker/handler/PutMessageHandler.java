package com.ipd.jmq.server.broker.handler;

import com.ipd.jmq.common.cluster.TopicConfig;
import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.*;
import com.ipd.jmq.common.model.JournalLog;
import com.ipd.jmq.common.network.v3.command.*;
import com.ipd.jmq.common.network.v3.session.*;
import com.ipd.jmq.server.broker.*;
import com.ipd.jmq.server.broker.archive.ArchiveManager;
import com.ipd.jmq.server.broker.cluster.ClusterManager;
import com.ipd.jmq.server.broker.dispatch.DispatchService;
import com.ipd.jmq.server.broker.monitor.BrokerMonitor;
import com.ipd.jmq.server.broker.utils.BrokerUtils;
import com.ipd.jmq.server.store.PutResult;
import com.ipd.jmq.server.store.Store;
import com.ipd.jmq.server.store.StoreContext;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.model.Acknowledge;
import com.ipd.jmq.common.network.v3.command.Command;
import com.ipd.jmq.toolkit.lang.Pair;
import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.jmq.toolkit.time.MilliPeriod;
import com.ipd.jmq.toolkit.time.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * 发送消息
 */
public class PutMessageHandler extends AbstractHandler implements JMQHandler {
    protected static final Logger logger = LoggerFactory.getLogger(PutMessageHandler.class);
    protected Store store;
    protected BrokerMonitor brokerMonitor;
    protected ClusterManager clusterManager;
    protected ArchiveManager archiveLogManager;
    protected DispatchService dispatchService;
    protected TxTransactionManager transactionManager;
    protected  BrokerConfig config;
    public  PutMessageHandler(){}
    public PutMessageHandler(ExecutorService executorService, SessionManager sessionManager,
                             ClusterManager clusterManager, DispatchService dispatchService, TxTransactionManager transactionManager,
                             BrokerConfig config, BrokerMonitor brokerMonitor) {

        Preconditions.checkArgument(config != null, "config can not be null");
        Preconditions.checkArgument(brokerMonitor != null, "brokerMonitor can not be null");
        Preconditions.checkArgument(clusterManager != null, "clusterManager can not be null");
        Preconditions.checkArgument(sessionManager != null, "sessionManager can not be null");
        Preconditions.checkArgument(dispatchService != null, "dispatchService can not be null");
        Preconditions.checkArgument(executorService != null, "executorService can not be null");
        Preconditions.checkArgument(transactionManager != null, "transactionManager can not be null");

        this.store = config.getStore();
        this.broker = clusterManager.getBroker();
        this.brokerMonitor = brokerMonitor;
        this.sessionManager = sessionManager;
        this.clusterManager = clusterManager;
        this.dispatchService = dispatchService;
        this.executorService = executorService;
        this.transactionManager = transactionManager;
    }

    public void setArchiveLogManager(ArchiveManager archiveLogManager) {
        this.archiveLogManager = archiveLogManager;
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }
    public void setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }
    public void setConfig(BrokerConfig config) {
        this.config = config;
        this.store = config.getStore();
    }

    public void setBrokerMonitor(BrokerMonitor brokerMonitor) {
        this.brokerMonitor = brokerMonitor;
    }

    public void setClusterManager(ClusterManager clusterManager) {
        this.broker = clusterManager.getBroker();
        this.clusterManager = clusterManager;
    }

    public void setDispatchService(DispatchService dispatchService) {
        this.dispatchService = dispatchService;
    }

    public void setTransactionManager(TxTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }


    @Override
    public Command process(final Transport transport, final Command command) throws TransportException {
        try {
            PutMessage putMessage = (PutMessage) command.getPayload();

            final Message[] messages = putMessage.getMessages();
            if (messages == null || messages.length == 0) {
                return CommandUtils.createBooleanAck(command.getHeader().getRequestId(), JMQCode.SUCCESS);
            }
            final TopicConfig topicConfig;
            final Command.Callback callback = (Command.Callback)command.getObject();
            // 容错，忽略连接和生产者不存在的异常
            String topic = messages[0].getTopic();
            String app = messages[0].getApp();
            Connection connection = null;
            if (callback == null) {
                // 获取连接和生产者信息
                ProducerId producerId = putMessage.getProducerId();
                Producer producer = sessionManager.getProducerById(producerId.getProducerId());
                ConnectionId connectionId = producerId.getConnectionId();
                connection = sessionManager.getConnectionById(connectionId.getConnectionId());

                if (connection == null) {
                    logger.warn(String.format("%s,Topic:%s,App:%s,ProducerId:%s", JMQCode.FW_CONNECTION_NOT_EXISTS.getMessage(),
                            topic, app, producerId.getProducerId()));
                } else {
                    app = connection.getApp();
                }
                if (producer == null) {
                    logger.warn(String.format("%s,Topic:%s,App:%s,ProducerId:%s", JMQCode.FW_PRODUCER_NOT_EXISTS.getMessage(),
                            topic, app, producerId.getProducerId()));
                } else {
                    topic = producer.getTopic();
                    app = producer.getApp();
                    JMQCode retCode = clusterManager.checkControllerWritable(topic, app, producerId.getConnectionId().getClientId().getIp());
                    if (retCode != JMQCode.SUCCESS) {
                        return CommandUtils.createBooleanAck(command.getHeader().getRequestId(), retCode);
                    }
                }

                // 判断是否能写
                topicConfig = clusterManager.checkWritable(topic, app);
                if (topicConfig.checkSequential() && topicConfig.singleProducer(app)) {
                    sessionManager.checkAndBindSequentialProducer(app, topic, producerId.getProducerId());
                }
            } else {
                topicConfig = clusterManager.getTopicConfig(topic);
            }

            // 客户端服务端地址
            Pair<byte[], byte[]> csAddress = new Pair<byte[], byte[]>(null, null);
            long now = SystemClock.now();
            final Acknowledge acknowledge = command.getHeader().getAcknowledge();
            // 遍历消息
            TransactionId transactionId = putMessage.getTransactionId();
            List<BrokerMessage> brokerMessages = new ArrayList<BrokerMessage>();
            int count = 0;
            int size = 0;
            for (Message message : messages) {
                // 确保主题一致
                if (!topic.equals(message.getTopic())) {
                    throw new JMQException("the put message command has multi-topic.",
                            JMQCode.FW_PUT_MESSAGE_ERROR.getCode());
                }
                // 设置期望发送到的队列
                if (putMessage.getQueueId() > 0) {
                    message.setQueueId(putMessage.getQueueId());
                }
                // 确保应用一致
                if (!app.equals(message.getApp())) {
                    message.setApp(app);
                }

                BrokerMessage bmsg = (BrokerMessage) message;
                if (transactionId != null) {
                    bmsg.setTxId(transactionId.getTransactionId());
                    bmsg.setType(JournalLog.TYPE_TX_PRE_MESSAGE);
                }
                BrokerUtils.setBrokerMessage(bmsg, csAddress, connection, transport, broker, now);
                dispatchService.dispatchQueue(bmsg);
                brokerMessages.add(bmsg);
                count++;
                size += message.getSize();
            }

            if (transactionId != null) {
                try {
                    transactionManager.addTxMessage(brokerMessages);
                } catch (Exception e) {
                    //如果事务消息发送失败则直接回滚事务
                    TxRollback rollback = new TxRollback();
                    rollback.setTopic(topic);
                    rollback.setTransactionId(transactionId);
                    transactionManager.TxRollback(rollback);
                    logger.error("deal failure ,rollback:" + transactionId.getTransactionId(), e);
                    if (e instanceof JMQException) {
                        throw (JMQException) e;
                    } else {
                        throw new JMQException(e, JMQCode.CN_UNKNOWN_ERROR.getCode());
                    }
                }
            } else {
                // 统计时间
                final MilliPeriod period = new MilliPeriod();
                period.begin();
                try {
                    final int messageCount = count;
                    final int messageSize = size;
                    final boolean isArchive = topicConfig.isArchive();
                    StoreContext storeContext = new StoreContext(topic, app, brokerMessages, null);
                    if (acknowledge == Acknowledge.ACK_FLUSH || isArchive) {
                        storeContext.setCallback(new Store.ProcessedCallback<BrokerMessage>() {
                            @Override
                            public void onProcessed(StoreContext<BrokerMessage> context) {
                                // 重要消息在从机复制成功后返回响应
                                try {
                                    int requestId = command.getHeader().getRequestId();
                                    JMQCode retCode = context.getResult().getCode();
                                    Command response = CommandUtils.createBooleanAck(requestId, retCode.getCode(), retCode.getMessage());
                                    if (retCode == JMQCode.SUCCESS && isArchive) {
                                        for (BrokerMessage brokerMessage : context.getLogs()) {
                                            if (topicConfig.isArchive()) {
                                                archiveLogManager.writeProduce(brokerMessage);
//                                                period.end();
//                                                brokerMonitor.onPutMessage(context.getTopic(), context.getApp(), messageCount, messageSize, (int) period.time());
                                            }
                                        }
                                    }
                                    if (callback == null) {
                                        if (acknowledge == Acknowledge.ACK_FLUSH) {
                                            transport.acknowledge(command, response, null);
                                        }
                                    } else {
                                        callback.execute(context.getResult());
                                    }
                                } catch (TransportException e) {
                                    logger.warn("send ack for {} failed: ", context.getTopic() + ":" + context.getApp(), e);
                                }
                            }
                        });
                    }else {
                        store.putJournalLogs(storeContext);
                        period.end();
                        brokerMonitor.onPutMessage(topic, app, messageCount, messageSize, (int) period.time());
                    }
                    if (acknowledge == Acknowledge.ACK_FLUSH) return null;
                } catch (JMQException e) {
                    if (count > 0) {
                        period.end();
                        brokerMonitor.onPutMessage(topic, app, count, size, (int) period.time());
                    }
                    brokerMonitor.onSendFailure();
                    throw e;
                }
            }

            if (acknowledge != Acknowledge.ACK_NO) {
                return CommandUtils.createBooleanAck(command.getHeader().getRequestId(), JMQCode.SUCCESS);
            }
        } catch (JMQException e) {
            throw new TransportException(e.getMessage(), e.getCause(), e.getCode());
        } catch (Exception e) {
            throw new TransportException(e.getMessage(), e.getCause(), JMQCode.CN_UNKNOWN_ERROR.getCode());
        }

        return null;
    }

    @Override
    public int[] type() {
        return new int[]{
                CmdTypes.PUT_MESSAGE,
                CmdTypes.PUT_MESSAGE_RICH};
    }
}