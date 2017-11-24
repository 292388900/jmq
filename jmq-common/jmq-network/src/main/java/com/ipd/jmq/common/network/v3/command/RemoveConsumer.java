package com.ipd.jmq.common.network.v3.command;

import com.ipd.jmq.common.network.v3.session.ConsumerId;
import com.ipd.jmq.toolkit.lang.Preconditions;

/**
 * 移除消费者
 */
public class RemoveConsumer extends JMQPayload {
    // 消费者ID
    private ConsumerId consumerId;

    public RemoveConsumer consumerId(final ConsumerId consumerId) {
        setConsumerId(consumerId);
        return this;
    }

    public ConsumerId getConsumerId() {
        return this.consumerId;
    }

    public void setConsumerId(ConsumerId consumerId) {
        this.consumerId = consumerId;
    }

    public int predictionSize() {
        return Serializer.getPredictionSize(consumerId.getConsumerId() + 1);
    }

    public void validate() {
        super.validate();
        Preconditions.checkArgument(consumerId != null, "consumer ID can not be null.");
    }

    @Override
    public int type() {
        return CmdTypes.REMOVE_CONSUMER;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RemoveConsumer{");
        sb.append("consumerId=").append(consumerId);
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

        RemoveConsumer that = (RemoveConsumer) o;

        if (consumerId != null ? !consumerId.equals(that.consumerId) : that.consumerId != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (consumerId != null ? consumerId.hashCode() : 0);
        return result;
    }
}