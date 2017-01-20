package com.ipd.jmq.server.broker.handler;

import com.ipd.jmq.common.cluster.TopicConfig;
import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.message.BrokerMessage;
import com.ipd.jmq.common.message.Message;
import com.ipd.jmq.common.network.v3.command.*;
import com.ipd.jmq.common.network.v3.session.*;
import com.ipd.jmq.server.broker.utils.BrokerUtils;
import com.ipd.jmq.server.broker.SessionManager;
import com.ipd.jmq.server.broker.TxTransactionManager;
import com.ipd.jmq.server.broker.cluster.ClusterManager;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.command.Command;
import com.ipd.jmq.common.network.command.Direction;
import com.ipd.jmq.toolkit.lang.Pair;
import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.jmq.toolkit.time.SystemClock;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;

/**
 * 事务处理器
 */
public class TxTransactionHandler extends AbstractHandler implements JMQHandler {
    private TxTransactionManager transactionManager;
    private ClusterManager clusterManager;

    public TxTransactionHandler(ExecutorService executorService, SessionManager sessionManager,
                                ClusterManager clusterManager, TxTransactionManager transactionManager) {

        Preconditions.checkArgument(clusterManager != null, "clusterManager can not be null");
        Preconditions.checkArgument(sessionManager != null, "sessionManager can not be null");
        Preconditions.checkArgument(executorService != null, "executorService can not be null");
        Preconditions.checkArgument(transactionManager != null, "transactionManager can not be null");

        this.broker = clusterManager.getBroker();
        this.sessionManager = sessionManager;
        this.clusterManager = clusterManager;
        this.executorService = executorService;
        this.transactionManager = transactionManager;
    }

    @Override
    public Command process(Transport transport, Command command) throws TransportException {
        try {
            JMQHeader header = (JMQHeader) command.getHeader();
            JMQPayload payload = (JMQPayload) command.getPayload();
            Transaction transaction = (Transaction) payload;

            //如果是事务开始消息，则设置事务ID
            int type = payload.type();
            if (transaction.getTransactionId() == null) {
                transaction.setTransactionId(new TransactionId(((TxPrepare) payload).getProducerId()));
            }

            TransactionId transactionId = transaction.getTransactionId();
            ProducerId producerId = transactionId.getProducerId();
            ConnectionId connectionId = producerId.getConnectionId();
            Connection connection = sessionManager.getConnectionById(connectionId.getConnectionId());
            if (connection == null) {
                throw new JMQException(JMQCode.FW_CONNECTION_NOT_EXISTS);
            }

            Producer producer = sessionManager.getProducerById(producerId.getProducerId());
            if (producer == null) {
                throw new JMQException(JMQCode.FW_PRODUCER_NOT_EXISTS);
            }

            String topic = producer.getTopic();
            String app = producer.getApp();

            // 判断是否能写
            TopicConfig topicConfig = clusterManager.checkWritable(topic, app);
            if (topicConfig.checkSequential() && topicConfig.singleProducer(app)) {
                sessionManager.checkAndBindSequentialProducer(app, topic, producerId.getProducerId());
            }

            Command response;
            switch (type) {
                case CmdTypes.TX_PREPARE: {
                    TxPrepare txPrepare = (TxPrepare) payload;
                    txPrepare.setTopic(topic);
                    List<Message> messages;
                    //设置超时时间
                    TopicConfig.ProducerPolicy policy = topicConfig.getProducerPolicy(app);
                    if (policy.getTxTimeout() != null && policy.getTxTimeout() > 0) {
                        txPrepare.setTimeout(policy.getTxTimeout());
                    }
                    transactionManager.TxPrepare(txPrepare);
                    try {
                        messages = txPrepare.getMessages();
                        if (messages != null && !messages.isEmpty()) {
                            List<BrokerMessage> preMessages = new ArrayList<BrokerMessage>();
                            long now = SystemClock.now();
                            Pair<byte[], byte[]> cs = new Pair<byte[], byte[]>(null, null);
                            for (Message message : messages) {
                                BrokerMessage preMessage = (BrokerMessage) message;
                                BrokerUtils.setBrokerMessage(preMessage, cs, connection, transport, broker, now);
                                preMessage.setTxId(txPrepare.getTransactionId().getTransactionId());
                                preMessages.add(preMessage);
                            }
                            transactionManager.addTxMessage(preMessages);
                        }

                    } catch (Throwable th) {
                        TxRollback rollback = new TxRollback();
                        rollback.setTopic(txPrepare.getTopic());
                        rollback.setTransactionId(txPrepare.getTransactionId());
                        transactionManager.TxRollback(rollback);
                        if (th instanceof JMQException) {
                            throw (JMQException) th;
                        } else {
                            throw new JMQException(JMQCode.CN_UNKNOWN_ERROR, th);
                        }
                    }
                    break;
                }
                case CmdTypes.TX_COMMIT: {
                    TxCommit txCommit = (TxCommit) payload;
                    txCommit.setTopic(topic);
                    transactionManager.TxCommit(txCommit, app);
                    break;
                }
                case CmdTypes.TX_ROLLBACK: {
                    TxRollback txRollback = (TxRollback) payload;
                    txRollback.setTopic(topic);
                    transactionManager.TxRollback(txRollback);
                    break;
                }
                default:
                    throw new JMQException(JMQCode.CN_COMMAND_UNSUPPORTED);
            }

            if (type == CmdTypes.TX_PREPARE) {
                TxPrepareAck ack = new TxPrepareAck();
                ack.setTransactionId(transactionId);
                ack.setTopic(topic);
                response = new Command(JMQHeader.Builder.create().direction(Direction.RESPONSE).requestId(header
                        .getRequestId()).version(header.getVersion()).type(ack.type()).status(JMQCode.SUCCESS.getCode()).build(), ack);
            } else {
                response = CommandUtils.createBooleanAck(header.getRequestId(), JMQCode.SUCCESS);
                JMQHeader jmqHeader = (JMQHeader) response.getHeader();
                jmqHeader.setVersion(header.getVersion());
            }
            return response;
        } catch (JMQException e) {
            throw new TransportException(e.getMessage(), e.getCause(), e.getCode());
        }
    }

    @Override
    public int[] type() {
        return new int[]{
                CmdTypes.TX_PREPARE,
                CmdTypes.TX_COMMIT,
                CmdTypes.TX_ROLLBACK};
    }
}