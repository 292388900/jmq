package com.ipd.jmq.common.network.v3.command;

/**
 * 事务查询确认.
 *
 * @author lindeqiang
 * @since 2016/5/16 17:11
 */
public class TxFeedbackAck extends Transaction {
    //查询ID
    private String queryId;
    //事务状态
    private TxStatus txStatus = TxStatus.PREPARE;
    //事务开始时间
    private long txStartTime;


    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public TxStatus getTxStatus() {
        return txStatus;
    }

    public void setTxStatus(TxStatus txStatus) {
        this.txStatus = txStatus;
    }

    public long getTxStartTime() {
        return txStartTime;
    }

    public void setTxStartTime(long txStartTime) {
        this.txStartTime = txStartTime;
    }

    @Override
    public int predictionSize() {
        return Serializer.getPredictionSize(transactionId == null? null : transactionId.getTransactionId(), queryId, topic) + 8 + 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        TxFeedbackAck feedback = (TxFeedbackAck) o;
        if (this.topic != null ? topic.equals(feedback.getTopic()) : feedback.getTopic() == null) return false;
        if (this.queryId != null ? queryId.equals(feedback.getQueryId()) : feedback.getQueryId() == null) return false;
        if (this.txStatus != null ? txStatus == feedback.getTxStatus() : feedback.getTxStatus() == null) return false;
        if (this.txStartTime != feedback.getTxStartTime()) return false;
        return true;
    }


    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (topic == null ? 0 : topic.hashCode());
        result = 31 * result + (queryId == null ? 0 : queryId.hashCode());
        result = 31 * result + (txStatus == null ? 0 : txStatus.hashCode());
        result = (int) (31 * result + txStartTime);
        return result;
    }

    @Override
    public String toString() {
        return "TxFeedbackAck{" +
                "queryId='" + queryId + '\'' +
                ", txStatus=" + txStatus +
                ", txStartTime=" + txStartTime +
                '}';
    }

    @Override
    public int type() {
        return CmdTypes.TX_FEEDBACK_ACK;
    }
}
