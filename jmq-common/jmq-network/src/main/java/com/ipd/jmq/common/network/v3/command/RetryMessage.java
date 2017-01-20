package com.ipd.jmq.common.network.v3.command;

import com.ipd.jmq.common.message.MessageLocation;
import com.ipd.jmq.common.network.v3.session.ConsumerId;
import com.ipd.jmq.toolkit.lang.Preconditions;

import java.util.Arrays;

/**
 * 客户端重试消息
 */
public class RetryMessage extends JMQPayload {
    // 消费者ID
    private ConsumerId consumerId;
    // 重试原因
    private String exception;
    // 位置
    private MessageLocation[] locations;


    public RetryMessage locations(final MessageLocation... locations) {
        setLocations(locations);
        return this;
    }

    public RetryMessage consumerId(final ConsumerId consumerId) {
        setConsumerId(consumerId);
        return this;
    }

    public RetryMessage exception(final String exception) {
        setException(exception);
        return this;
    }

    public ConsumerId getConsumerId() {
        return this.consumerId;
    }

    public void setConsumerId(ConsumerId consumerId) {
        this.consumerId = consumerId;
    }

    public MessageLocation[] getLocations() {
        return this.locations;
    }

    public void setLocations(MessageLocation[] locations) {
        this.locations = locations;
    }

    public String getException() {
        return exception;
    }

    public void setException(String exception) {
        this.exception = exception;
    }


    public int predictionSize() {
        int size = Serializer.getPredictionSize(consumerId.getConsumerId()) + Serializer
                .getPredictionSize(exception, 2);
        size += Serializer.getPredictionSize(locations);
        size += 1;
        return size;
    }

    @Override
    public void validate() {
        super.validate();
        Preconditions.checkArgument(consumerId != null, "consumer ID can not be null.");
        Preconditions.checkArgument(locations != null && locations.length > 0, "locations can not be empty.");
    }

    @Override
    public int type() {
        return CmdTypes.RETRY_MESSAGE;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RetryMessage{");
        sb.append("consumerId=").append(consumerId);
        sb.append(", exception='").append(exception).append('\'');
        sb.append(", locations=").append(Arrays.toString(locations));
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

        RetryMessage that = (RetryMessage) o;

        if (consumerId != null ? !consumerId.equals(that.consumerId) : that.consumerId != null) {
            return false;
        }
        if (exception != null ? !exception.equals(that.exception) : that.exception != null) {
            return false;
        }
        if (!Arrays.equals(locations, that.locations)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (consumerId != null ? consumerId.hashCode() : 0);
        result = 31 * result + (exception != null ? exception.hashCode() : 0);
        result = 31 * result + (locations != null ? Arrays.hashCode(locations) : 0);
        return result;
    }
}