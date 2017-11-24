package com.ipd.jmq.common.network.v3.command;

import com.ipd.jmq.common.message.Message;
import com.ipd.jmq.common.network.v3.session.ProducerId;
import com.ipd.jmq.common.network.v3.session.TransactionId;
import com.ipd.jmq.toolkit.lang.Preconditions;

import java.util.Arrays;

/**
 * 生产消息
 */
public class PutMessage extends JMQPayload {
    // 生产者ID
    private ProducerId producerId;
    // 事务ID
    private TransactionId transactionId;
    // 消息数组
    private Message[] messages;
    // 队列ID
    private short queueId;

    public ProducerId getProducerId() {
        return this.producerId;
    }

    public void setProducerId(ProducerId producerId) {
        this.producerId = producerId;
    }

    public PutMessage queueId(final short queueId) {
        setQueueId(queueId);
        return this;
    }

    public PutMessage transactionId(final TransactionId transactionId) {
        setTransactionId(transactionId);
        return this;
    }

    public PutMessage producerId(final ProducerId producerId) {
        setProducerId(producerId);
        return this;
    }

    public PutMessage messages(final Message... messages) {
        setMessages(messages);
        return this;
    }

    public TransactionId getTransactionId() {
        return this.transactionId;
    }

    public void setTransactionId(TransactionId transactionId) {
        this.transactionId = transactionId;
    }

    public Message[] getMessages() {
        return this.messages;
    }

    public void setMessages(Message[] messages) {
        this.messages = messages;
    }

    public short getQueueId() {
        return queueId;
    }

    public void setQueueId(short queueId) {
        this.queueId = queueId;
    }

    @Override
    public int predictionSize() {
        int size = Serializer.getPredictionSize(producerId.getProducerId());
        if (transactionId != null) {
            size += Serializer.getPredictionSize(transactionId.getTransactionId());
        } else {
            size += 1;
        }
        size += Serializer.getPredictionSize(messages);
        return size + 1;
    }

    @Override
    public void validate() {
        super.validate();
        Preconditions.checkArgument(producerId != null, "productId must note be null");
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PutMessage{");
        sb.append("producerId=").append(producerId);
        sb.append(", transactionId=").append(transactionId);
        sb.append(", messages=").append(Arrays.toString(messages));
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        PutMessage that = (PutMessage) o;

        if (!Arrays.equals(messages, that.messages)) {
            return false;
        }

        if (producerId != null ? !producerId.equals(that.producerId) : that.producerId != null) {
            return false;
        }
        if (transactionId != null ? !transactionId.equals(that.transactionId) : that.transactionId != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (producerId != null ? producerId.hashCode() : 0);
        result = 31 * result + (transactionId != null ? transactionId.hashCode() : 0);
        result = 31 * result + (messages != null ? Arrays.hashCode(messages) : 0);
        return result;
    }

    @Override
    public int type() {
        return CmdTypes.PUT_MESSAGE;
    }
}