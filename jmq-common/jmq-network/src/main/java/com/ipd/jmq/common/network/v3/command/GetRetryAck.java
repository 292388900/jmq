package com.ipd.jmq.common.network.v3.command;

import com.ipd.jmq.common.message.BrokerMessage;

import java.util.Arrays;

/**
 * 重试应答
 *
 * @author Jame.HU
 * @version V1.0
 */
public class GetRetryAck extends JMQPayload {
    // 存储的消息
    protected BrokerMessage[] messages;

    @Override
    public int type() {
        return CmdTypes.GET_RETRY_ACK;
    }

    public GetRetryAck messages(final BrokerMessage[] messages) {
        this.messages = messages;
        return this;
    }

    public BrokerMessage[] getMessages() {
        return messages;
    }

    public void setMessages(BrokerMessage[] messages) {
        this.messages = messages;
    }

    public int predictionSize() {
        return Serializer.getPredictionSize(messages) + 1;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GetRetryAck{");
        sb.append("messages=").append(Arrays.toString(messages));
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

        GetRetryAck that = (GetRetryAck) o;

        if (!Arrays.equals(messages, that.messages)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (messages != null ? Arrays.hashCode(messages) : 0);
        return result;
    }
}
