package com.ipd.jmq.common.network.v3.command;

import com.ipd.jmq.toolkit.validate.ValidateException;

import java.util.HashMap;
import java.util.Map;

/**
 * 事务提交命令
 *
 * @author dingjun
 * @since 16-5-5.
 */
public class TxCommit extends Transaction {
    //事务结束时间
    private long endTime;
    //属性列表
    private Map<Object, Object> attrs = new HashMap<Object, Object>();

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Map<Object, Object> getAttrs() {
        return attrs;
    }

    public void setAttrs(Map<Object, Object> attrs) {
        this.attrs = attrs;
    }

    @Override
    public int predictionSize() {
        return 1 + 8 + Serializer.getPredictionSize(transactionId.getTransactionId(), topic) + Serializer
                .getPredictionSize(attrs);
    }

    @Override
    public void validate() {
        if (null == transactionId) {
            throw new ValidateException("transactionId could not be null!");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        TxCommit txCommit = (TxCommit) o;
        if (this.topic != null ? !topic.equals(txCommit.getTopic()) : txCommit.getTopic() == null) return false;
        if (this.attrs != null ? !attrs.equals(txCommit.getAttrs()) : txCommit.getAttrs() == null) return false;
        return endTime == txCommit.endTime;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (int) (endTime ^ (endTime >>> 32));
        result = 31 * result + (topic == null ? 0 : topic.hashCode());
        result = 31 * result + (attrs == null ? 0 : attrs.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return "TxCommit{" +
                "endTime=" + endTime +
                ", attrs=" + attrs +
                '}';
    }

    @Override
    public int type() {
        return CmdTypes.TX_COMMIT;
    }
}
