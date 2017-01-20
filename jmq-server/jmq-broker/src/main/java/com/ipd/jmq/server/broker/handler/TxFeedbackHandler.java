package com.ipd.jmq.server.broker.handler;

import com.ipd.jmq.common.exception.JMQCode;
import com.ipd.jmq.common.exception.JMQException;
import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.JMQHeader;
import com.ipd.jmq.common.network.v3.command.TxFeedback;
import com.ipd.jmq.common.network.v3.command.TxFeedbackAck;
import com.ipd.jmq.common.network.v3.session.*;
import com.ipd.jmq.server.broker.SessionManager;
import com.ipd.jmq.server.broker.TxTransactionManager;
import com.ipd.jmq.server.broker.cluster.ClusterManager;
import com.ipd.jmq.server.store.GetTxResult;
import com.ipd.jmq.common.network.Transport;
import com.ipd.jmq.common.network.TransportException;
import com.ipd.jmq.common.network.command.Command;
import com.ipd.jmq.common.network.command.Direction;
import com.ipd.jmq.toolkit.lang.Preconditions;
import com.ipd.jmq.toolkit.time.SystemClock;

import java.util.concurrent.ExecutorService;

/**
 * 事务补偿处理器
 */
public class TxFeedbackHandler extends AbstractHandler implements JMQHandler {
    private TxTransactionManager transactionManager;

    public TxFeedbackHandler(ExecutorService executorService, SessionManager sessionManager,
                             ClusterManager clusterManager, TxTransactionManager transactionManager) {

        Preconditions.checkArgument(clusterManager != null, "clusterManager can not be null");
        Preconditions.checkArgument(sessionManager != null, "sessionManager can not be null");
        Preconditions.checkArgument(executorService != null, "executorService can not be null");
        Preconditions.checkArgument(transactionManager != null, "transactionManager can not be null");

        this.broker = clusterManager.getBroker();
        this.sessionManager = sessionManager;
        this.executorService = executorService;
        this.transactionManager = transactionManager;
    }

    @Override
    public Command process(Transport transport, Command command) throws TransportException {
        try {
            final TxFeedback feedback = (TxFeedback) command.getPayload();
            ProducerId producerId = feedback.getProducerId();
            ConnectionId connectionId = producerId.getConnectionId();
            Connection connection = sessionManager.getConnectionById(connectionId.getConnectionId());
            Producer producer = sessionManager.getProducerById(producerId.getProducerId());
            if (connection == null) {
                throw new JMQException(JMQCode.FW_CONNECTION_NOT_EXISTS);
            }

            if (producer == null) {
                throw new JMQException(JMQCode.FW_PRODUCER_NOT_EXISTS);
            }

            if (!broker.isWritable()) {
                throw new JMQException(broker.getName() + " is not writable", JMQCode.CN_NO_PERMISSION.getCode());
            }

            long currentTime = SystemClock.now();
            //处理事务补偿
            if (feedback.getTransactionId() != null) {
                transactionManager.txFeedBack(feedback);
            }

            //处理查询
            GetTxResult result = transactionManager.getInflightTransaction(feedback);
            TxFeedbackAck ack = new TxFeedbackAck();

            Command response = new Command(JMQHeader.Builder.create().direction(Direction.RESPONSE).type(ack.type())
                    .build(), ack);
            if (result == null && feedback.getLongPull() > 0) {
                long cost = currentTime - SystemClock.now();
                feedback.setLongPull((int) (feedback.getLongPull() - cost));
                if (feedback.getLongPull() > 0 && transactionManager.addQueryToLongPull(command, transport)) {
                    response = null;
                }
            } else if (result != null) {
                ack.setQueryId(result.getQueryId());
                ack.setTopic(result.getTopic());
                ack.setTransactionId(new TransactionId(result.getTxId()));
            }

            return response;


        } catch (JMQException e) {
            throw new TransportException(e.getMessage(), e.getCause(),e.getCode());
        }
    }

    @Override
    public int[] type() {
        return new int[]{CmdTypes.TX_FEEDBACK};
    }
}