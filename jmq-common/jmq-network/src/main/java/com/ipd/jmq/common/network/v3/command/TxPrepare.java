package com.ipd.jmq.common.network.v3.command;

import com.ipd.jmq.common.message.Message;
import com.ipd.jmq.common.network.v3.session.ProducerId;
import com.ipd.jmq.toolkit.lang.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author dingjun
 * @since 16-5-5.
 */
public class TxPrepare extends Transaction {
    public static final byte TIME_OUT_ACTION_ROLLBACK = 1;
    public static final byte TIME_OUT_ACTION_FEEDBACK = 2;
    private ProducerId producerId;
    private String queryId;
    private long startTime;
    private int timeout;
    private byte timeoutAction = TIME_OUT_ACTION_FEEDBACK;
    private Map<Object, Object> attrs = new HashMap<Object, Object>();
    private List<Message> messages = new ArrayList<Message>();


    @Override
    public int predictionSize() {
        return Serializer.getPredictionSize(transactionId != null ? transactionId.getTransactionId() : null,
                producerId.getProducerId(), queryId) + 8 + 4 + 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        TxPrepare txPrepare = (TxPrepare) o;

        if (this.startTime != txPrepare.getStartTime()) return false;
        if (this.timeout != txPrepare.getTimeout()) return false;

        if (this.topic != null ? !this.topic.equals(txPrepare.getTopic()) : txPrepare.getTopic() == null) return false;

        if (this.queryId != null ? !this.queryId.equals(txPrepare.getQueryId()) : txPrepare.getQueryId() == null) {
            return false;
        }

        if (this.attrs != null ? !attrs.equals(txPrepare.getAttrs()) : txPrepare.getAttrs() == null)
            return false;

        if (this.producerId != null ? !producerId.equals(txPrepare.getProducerId()) : txPrepare.getProducerId() == null)
            return false;

        return true;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (int) (this.startTime ^ (this.startTime >>> 32));
        result = 31 * result + (this.timeout ^ (this.timeout >>> 32));
        result = 31 * result + (topic == null ? 0 : topic.hashCode());
        result = 31 * result + (attrs == null ? 0 : attrs.hashCode());
        result = 31 * result + (queryId == null ? 0 : queryId.hashCode());
        result = 31 * result + (producerId == null ? 0 : producerId.hashCode());
        result = 31 * result + (this.timeoutAction ^ (this.timeoutAction >>> 32));
        return result;

    }

    @Override
    public String toString() {
        return "TxPrepare{" +
                "topic='" + topic + '\'' +
                ", producerId=" + producerId +
                ", queryId='" + queryId + '\'' +
                ", startTime=" + startTime +
                ", timeout=" + timeout +
                ", timeoutAction=" + timeoutAction +
                ", messages=" + messages +
                '}';
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public List<Message> getMessages() {
        return messages;
    }

    public void setMessages(List<Message> messages) {
        this.messages = messages;
    }


    public Map<Object, Object> getAttrs() {
        return attrs;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public ProducerId getProducerId() {
        return producerId;
    }

    public void setProducerId(ProducerId producerId) {
        this.producerId = producerId;
    }

    public byte getTimeoutAction() {
        return timeoutAction;
    }

    public void setTimeoutAction(byte timeoutAction) {
        this.timeoutAction = timeoutAction;
    }

    /**
     * 数据校验
     */
    @Override
    public void validate() {
        Preconditions.checkArgument(null != topic && !topic.isEmpty(), "topic could not be null or empty.");
        Preconditions.checkArgument(null != producerId, "producer ID can not be null.");
        Preconditions.checkArgument(startTime > 1, "start time must bigger than 1.");
        Preconditions.checkArgument(timeout > 1, "time out must bigger than 1.");

    }

    @Override
    public int type() {
        return CmdTypes.TX_PREPARE;
    }
}
