package com.ipd.jmq.common.network.v3.command;

import com.ipd.jmq.toolkit.lang.Preconditions;

/**
 * @author dingjun
 * @since 16-5-5.
 */
public class TxRollback extends Transaction {

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Override
    public int predictionSize() {
        return Serializer.getPredictionSize(topic) + 1;
    }

    @Override
    public int type() {
        return CmdTypes.TX_ROLLBACK;
    }

    @Override
    public void validate() {
        super.validate();
        Preconditions.checkArgument(null != transactionId, "transactionId could not be null.");
        Preconditions.checkArgument(null != topic && !topic.isEmpty(), "topic could not be null or empty.");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        TxRollback that = (TxRollback) o;

        if (this.topic != null ? !this.topic.equals(that.getTopic()) : that.getTopic() == null) return false;
        if (this.transactionId != null ? !this.transactionId.equals(that.getTransactionId()) : that.getTransactionId() == null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + topic.hashCode();
        result = 31 * result + transactionId.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "TxRollback{" +
                "topic='" + topic + '\'' +
                "transactionId='" + transactionId.getTransactionId() + '\'' +
                '}';
    }
}
