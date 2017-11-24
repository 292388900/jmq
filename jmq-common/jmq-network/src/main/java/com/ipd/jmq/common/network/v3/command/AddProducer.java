package com.ipd.jmq.common.network.v3.command;

import com.ipd.jmq.common.network.v3.session.ProducerId;
import com.ipd.jmq.toolkit.lang.Preconditions;

/**
 * 添加生产者
 */
public class AddProducer extends JMQPayload {
    // 生产者ID
    private ProducerId producerId;
    // 主题
    private String topic;

    public AddProducer topic(final String topic) {
        setTopic(topic);
        return this;
    }

    public AddProducer producerId(final ProducerId producerId) {
        setProducerId(producerId);
        return this;
    }

    public ProducerId getProducerId() {
        return this.producerId;
    }

    public void setProducerId(ProducerId producerId) {
        this.producerId = producerId;
    }

    public String getTopic() {
        return this.topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Override
    public int predictionSize() {
        return Serializer.getPredictionSize(producerId.getProducerId(), topic) + 1;
    }

    @Override
    public void validate() {
        super.validate();
        Preconditions.checkArgument(producerId != null, "producer ID can not be null");
        Preconditions.checkArgument(topic != null && !topic.isEmpty(), "topic can not be null");
    }

    @Override
    public int type() {
        return CmdTypes.ADD_PRODUCER;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AddProducer{");
        sb.append("producerId=").append(producerId);
        sb.append(", topic='").append(topic).append('\'');
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

        AddProducer that = (AddProducer) o;

        if (producerId != null ? !producerId.equals(that.producerId) : that.producerId != null) {
            return false;
        }
        if (topic != null ? !topic.equals(that.topic) : that.topic != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = producerId != null ? producerId.hashCode() : 0;
        result = 31 * result + (topic != null ? topic.hashCode() : 0);
        return result;
    }
}