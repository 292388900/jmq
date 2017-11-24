package com.ipd.jmq.common.network.v3.command;

import com.ipd.jmq.common.network.v3.session.ConsumerId;
import com.ipd.jmq.toolkit.lang.Preconditions;

/**
 * 添加消费者
 */
public class AddConsumer extends JMQPayload {
    // 消费者ID
    private ConsumerId consumerId;
    // 主题
    private String topic;
    // 选择器
    private String selector;

    public AddConsumer topic(final String topic) {
        setTopic(topic);
        return this;
    }

    public AddConsumer selector(final String selector) {
        setSelector(selector);
        return this;
    }

    public AddConsumer consumerId(final ConsumerId consumerId) {
        setConsumerId(consumerId);
        return this;
    }

    public ConsumerId getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(ConsumerId consumerId) {
        this.consumerId = consumerId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getSelector() {
        return selector;
    }

    public void setSelector(String selector) {
        this.selector = selector;
    }

    public int predictionSize() {
        return Serializer.getPredictionSize(consumerId.getConsumerId(), topic) + Serializer
                .getPredictionSize(selector, 2) +1;
    }

    @Override
    public void validate() {
        super.validate();
        Preconditions.checkArgument(consumerId != null, "consumer ID can not be null");
        Preconditions.checkArgument(topic != null && !topic.isEmpty(), "topic can not be null");
    }

    @Override
    public int type() {
        return CmdTypes.ADD_CONSUMER;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AddConsumer{");
        sb.append("consumerId=").append(consumerId);
        sb.append(", topic='").append(topic).append('\'');
        sb.append(", selector='").append(selector).append('\'');
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

        AddConsumer that = (AddConsumer) o;

        if (consumerId != null ? !consumerId.equals(that.consumerId) : that.consumerId != null) {
            return false;
        }
        if (selector != null ? !selector.equals(that.selector) : that.selector != null) {
            return false;
        }
        if (topic != null ? !topic.equals(that.topic) : that.topic != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = consumerId != null ? consumerId.hashCode() : 0;
        result = 31 * result + (topic != null ? topic.hashCode() : 0);
        result = 31 * result + (selector != null ? selector.hashCode() : 0);
        return result;
    }
}