package com.ipd.jmq.server.broker.handler;

import com.ipd.jmq.common.cluster.TopicConfig;
import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.MessageLocation;
import com.ipd.jmq.common.message.MessageQueue;
import com.ipd.jmq.common.message.QueueItem;
import com.ipd.jmq.common.network.v3.command.*;
import com.ipd.jmq.common.network.v3.session.Connection;
import com.ipd.jmq.common.network.v3.session.ConnectionId;
import com.ipd.jmq.common.network.v3.session.Consumer;
import com.ipd.jmq.common.network.v3.session.ConsumerId;
import com.ipd.jmq.server.broker.BrokerConfig;
import com.ipd.jmq.server.broker.SessionManager;
import com.ipd.jmq.server.broker.archive.ArchiveManager;
import com.ipd.jmq.server.broker.cluster.ClusterManager;
import com.ipd.jmq.server.broker.dispatch.DispatchService;
import com.ipd.jmq.server.broker.monitor.BrokerMonitor;
import com.ipd.jmq.server.broker.retry.RetryManager;
import com.ipd.jmq.server.store.Store;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.toolkit.lang.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * 消费确认处理。
 */
public class AckMessageHandler extends AbstractHandler implements JMQHandler {
    private static final Logger logger = LoggerFactory.getLogger(AckMessageHandler.class);
    private Store store;
    private RetryManager retryManager;
    private DispatchService dispatchService;
    private ClusterManager clusterManager;
    private ArchiveManager archiveManager;
    protected BrokerMonitor brokerMonitor;
    private BrokerConfig config;

    public AckMessageHandler(){}
    public AckMessageHandler(ExecutorService executorService, SessionManager sessionManager,
                             ClusterManager clusterManager, DispatchService dispatchService, Store store, BrokerMonitor brokerMonitor) {

        Preconditions.checkArgument(store != null, "store can not be null");
        Preconditions.checkArgument(brokerMonitor != null, "brokerMonitor can not be null");
        Preconditions.checkArgument(clusterManager != null, "clusterManager can not be null");
        Preconditions.checkArgument(sessionManager != null, "sessionManager can not be null");
        Preconditions.checkArgument(dispatchService != null, "dispatchService can not be null");
        Preconditions.checkArgument(executorService != null, "executorService can not be null");

        this.executorService = executorService;
        this.sessionManager = sessionManager;
        this.clusterManager = clusterManager;
        this.broker = clusterManager.getBroker();
        this.dispatchService = dispatchService;
        this.store = store;
        this.brokerMonitor = brokerMonitor;
    }

    public void setConfig(BrokerConfig config) {
        this.config = config;
        this.store = config.getStore();
    }

    public void setArchiveManager(ArchiveManager archiveManager) {
        this.archiveManager = archiveManager;
    }

    public void setRetryManager(RetryManager retryManager) {
        this.retryManager = retryManager;
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }
    public void setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }
    public void setDispatchService(DispatchService dispatchService) {
        this.dispatchService = dispatchService;
    }

    public void setClusterManager(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
        this.broker = clusterManager.getBroker();
    }

    public void setBrokerMonitor(BrokerMonitor brokerMonitor) {
        this.brokerMonitor = brokerMonitor;
    }

    @Override
    public Command process(Transport transport, Command command) throws TransportException {
        try {
            if (!broker.isReadable()) {
                throw new JMQException(broker.getName() + " is not readable", JMQCode.CN_NO_PERMISSION.getCode());
            }

            Header header = command.getHeader();
            AckMessage ackMessage = (AckMessage) command.getPayload();

            ConsumerId consumerId = ackMessage.getConsumerId();
            ConnectionId connectionId = consumerId.getConnectionId();
            Consumer consumer = sessionManager.getConsumerById(consumerId.getConsumerId());
            Connection connection = sessionManager.getConnectionById(connectionId.getConnectionId());
            if (connection == null) {
                throw new JMQException(JMQCode.FW_CONNECTION_NOT_EXISTS);
            }
            if (consumer == null) {
                throw new JMQException(JMQCode.FW_CONSUMER_NOT_EXISTS);
            }
            // 获取应答消息
            MessageLocation[] locations = ackMessage.getLocations();
            if (locations != null && locations.length > 0) {
                // 应答消息的主题必须一致
                String topic = locations[0].getTopic();
                TopicConfig topicConfig = clusterManager.getTopicConfig(topic);
                boolean isArchive = topicConfig == null ? false : topicConfig.isArchive(consumer.getApp());
                List<MessageLocation> retryLocations = new ArrayList<MessageLocation>(locations.length);
                List<MessageLocation> ackLocations = new ArrayList<MessageLocation>(locations.length);
                // 消息来源分组
                for (MessageLocation location : locations) {
                    if (location.getQueueId() == MessageQueue.RETRY_QUEUE) {
                        // 重试消息
                        retryLocations.add(location);
                    } else {
                        // 普通消息
                        ackLocations.add(location);
                    }
                }
                if (!ackLocations.isEmpty()) {
                    // 普通消息应答
                    dispatchService.acknowledge(ackLocations.toArray(new MessageLocation[ackLocations.size()]), consumer, true);
                    //应答一次成功就认为消费者可以正常消费了
                    if (isArchive) {
                        for (MessageLocation location : locations) {
                            // 兼容AMQ客户端应答，批量应答，可能获取不了日志偏移量
                            if (location.getJournalOffset() == -1) {
                                try {
                                    // 重新从存储获取
                                    QueueItem queueItem = store.getQueueItem(location.getTopic(), location.getQueueId(),
                                            location.getQueueOffset());
                                    if (queueItem != null) {
                                        location.setJournalOffset(queueItem.getJournalOffset());
                                    }
                                } catch (JMQException e) {
                                    logger.error("get queue item error", e);
                                }
                            }
                            if(null!=archiveManager) {
                                archiveManager.writeConsume(connection, location);
                            }
                        }
                    }
                }
                if (!retryLocations.isEmpty()) {
                    // 重试消息应答
                    try {
                        long[] retryIds = new long[retryLocations.size()];
                        int count = 0;
                        for (MessageLocation location : locations) {
                            retryIds[count++] = location.getQueueOffset();
                        }
                        // 忽略重试异常
                        if(null!=retryManager) {
                            retryManager.retrySuccess(topic, connection.getApp(), retryIds);
                        }
                        //监控
                        brokerMonitor.onRetrySuccess(topic, connection.getApp(), retryIds.length);

                        if (isArchive) {
                            for (MessageLocation location : locations) {
                                if(null!=archiveManager) {
                                    archiveManager.writeConsume(connection, location);
                                }
                            }
                        }
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                        return CommandUtils.createBooleanAck(header.getRequestId(), JMQCode.FW_CONSUMER_ACK_FAIL.getCode(),
                                e.getMessage());
                    } finally {
                        dispatchService
                                .acknowledge(retryLocations.toArray(new MessageLocation[retryLocations.size()]), consumer, false);
                    }
                }
            }
            return CommandUtils.createBooleanAck(header.getRequestId(), JMQCode.SUCCESS);

        } catch (JMQException e) {
            throw new TransportException(e.getMessage(), e.getCause(),e.getCode());
        }
    }

    @Override
    public int[] type() {
        return new int[]{CmdTypes.ACK_MESSAGE};
    }
}