package com.ipd.kafka.cluster;

/**
 * Created by zhangkepeng on 16-8-24.
 */
public class TopicsBrokerEvent extends KafkaClusterEvent {

    private String topic;

    private int partition;

    private int lastBrokerId;

    private int controllerEpoch;

    public TopicsBrokerEvent(Builder builder) {
        this.topic = builder.topic;
        this.type = builder.eventType;
        this.partition = builder.partition;
        this.lastBrokerId = builder.lastBrokerId;
        this.controllerEpoch = builder.controllerEpoch;
    }

    public int getLastBrokerId() {
        return lastBrokerId;
    }

    public void setLastBrokerId(int lastBrokerId) {
        this.lastBrokerId = lastBrokerId;
    }

    public int getControllerEpoch() {
        return controllerEpoch;
    }

    public void setControllerEpoch(int controllerEpoch) {
        this.controllerEpoch = controllerEpoch;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public static class Builder {

        private String topic;

        private EventType eventType;

        private int partition;

        private int lastBrokerId;

        private int controllerEpoch;

        public Builder(String topic, EventType eventType) {
            this.topic = topic;
            this.eventType = eventType;
        }

        public Builder partition(int partition) {
            this.partition = partition;
            return this;
        }

        public Builder lastBrokerId(int lastBrokerId) {
            this.lastBrokerId = lastBrokerId;
            return this;
        }

        public Builder controllerEpoch(int controllerEpoch) {
            this.controllerEpoch = controllerEpoch;
            return this;
        }

        public TopicsBrokerEvent build() {
            return new TopicsBrokerEvent(this);
        }
    }
}
