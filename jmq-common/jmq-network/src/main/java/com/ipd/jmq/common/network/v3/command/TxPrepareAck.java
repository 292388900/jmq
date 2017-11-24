package com.ipd.jmq.common.network.v3.command;

import com.ipd.jmq.common.network.v3.session.TransactionId;

/**
 * @author dingjun
 * @since 16-5-5.
 */
public class TxPrepareAck extends Transaction {

    @Override
    public int predictionSize() {
        return 1 + 1 + Serializer.getPredictionSize(transactionId != null ? transactionId.getTransactionId() : null, 2);
    }

    public TransactionId getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(TransactionId transactionId) {
        this.transactionId = transactionId;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTopic() {
        return this.topic;
    }


    @Override
    public String toString() {
        return "TxPrepareAck{" +
                "topic=" + topic +
                ", transactionId=" + (transactionId == null ? "null" : transactionId.getTransactionId()) +
                '}';
    }

    @Override
    public int type() {
        return CmdTypes.TX_PREPARE_ACK;
    }
}
