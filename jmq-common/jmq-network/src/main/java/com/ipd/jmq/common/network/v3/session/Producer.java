package com.ipd.jmq.common.network.v3.session;

/**
 * 生产者
 */
public class Producer extends Joint {
    // 生产者ID
    private String id;
    // 连接ID
    private String connectionId;
    // 生产类型
    private ProducerType type = ProducerType.JMQ;

    public Producer() {
    }

    public Producer(String id, String connectionId, String topic) {
        this.id = id;
        this.connectionId = connectionId;
        this.topic = topic;
    }

    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getConnectionId() {
        return this.connectionId;
    }

    public void setConnectionId(String connectionId) {
        this.connectionId = connectionId;
    }

    public enum ProducerType {
        JMQ,
        KAFKA;
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

        Producer producer = (Producer) o;

        if (connectionId != null ? !connectionId.equals(producer.connectionId) : producer.connectionId != null) {
            return false;
        }
        if (id != null ? !id.equals(producer.id) : producer.id != null) {
            return false;
        }
        if (type != null ?type.equals(producer.type) : producer.type != null ) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (id != null ? id.hashCode() : 0);
        result = 31 * result + (connectionId != null ? connectionId.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        return result;
    }
}