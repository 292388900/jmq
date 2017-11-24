package com.ipd.jmq.common.network.v3.command;

import com.ipd.jmq.common.network.v3.session.ProducerId;

/**
 * 事务查询以及补偿.
 *
 * @author lindeqiang
 * @since 2016/5/16 17:11
 */
public class TxFeedback extends Transaction {
    private String app;
    private ProducerId producerId;
    private TxStatus txStatus = TxStatus.UNKNOWN;
    private int longPull = 0;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public TxStatus getTxStatus() {
        return txStatus;
    }

    public void setTxStatus(TxStatus txStatus) {
        this.txStatus = txStatus;
    }

    public ProducerId getProducerId() {
        return producerId;
    }

    public void setProducerId(ProducerId producerId) {
        this.producerId = producerId;
    }

    public int getLongPull() {
        return longPull;
    }

    public void setLongPull(int longPull) {
        this.longPull = longPull;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    @Override
    public int predictionSize() {
        return 1 + 1 + 4 + Serializer.getPredictionSize(topic, app, producerId.getProducerId(),
                null != transactionId? transactionId.getTransactionId() : null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        TxFeedback txQuery = (TxFeedback) o;
        if (this.topic != null ? topic.equals(txQuery.getTopic()) : txQuery.getTopic() == null) return false;
        if (this.txStatus != null ? txStatus == txQuery.getTxStatus() : txQuery.getTxStatus() == null) return false;
        if (this.app != null ? app.equals(txQuery.getApp()) : txQuery.getApp() == null) return false;
        if (this.longPull != txQuery.getLongPull()) return false;

        return true;
    }


    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (topic == null ? 0 : topic.hashCode());
        result = 31 * result + (app == null ? 0 : app.hashCode());
        result = 31 * result + (txStatus == null ? 0 : txStatus.hashCode());
        result = 31 * result + longPull;
        return result;
    }

    @Override
    public String toString() {
        return "TxFeedback{" +
                "app='" + app + '\'' +
                ", producerId=" + producerId +
                ", txStatus=" + txStatus +
                ", longPull=" + longPull +
                '}';
    }

    @Override
    public int type() {
        return CmdTypes.TX_FEEDBACK;
    }
}
