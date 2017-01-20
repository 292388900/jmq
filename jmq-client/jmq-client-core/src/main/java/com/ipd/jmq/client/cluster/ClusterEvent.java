package com.ipd.jmq.client.cluster;


import com.ipd.jmq.common.cluster.BrokerGroup;

/**
 * 集群事件
 *
 * @author lindeqiang
 * @since 14-4-29 上午10:37
 */
public class ClusterEvent {
    // 主题
    private String topic;
    // 分组
    private BrokerGroup group;
    // 队列数
    private short queues;
    // 类型
    private EventType type;

    public ClusterEvent() {
    }

    public ClusterEvent(EventType type, String topic, BrokerGroup group) {
        this(type, topic, group, (short) 0);
    }

    public ClusterEvent(EventType type, String topic, short queues) {
        this(type, topic, null, queues);
    }

    public ClusterEvent(EventType type, String topic, BrokerGroup group, short queues) {
        setType(type);
        setTopic(topic);
        setGroup(group);
        setQueues(queues);
    }


    public String getTopic() {
        return this.topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public BrokerGroup getGroup() {
        return this.group;
    }

    public void setGroup(BrokerGroup group) {
        this.group = group;
    }

    public EventType getType() {
        return this.type;
    }

    public void setType(EventType type) {
        this.type = type;
    }

    public short getQueues() {
        return queues;
    }

    public void setQueues(short queues) {
        this.queues = queues;
    }

    public enum EventType {
        /**
         * 增加Broker
         */
        ADD_BROKER,
        /**
         * 删除Broker
         */
        REMOVE_BROKER,
        /**
         * 更新Broker
         */
        UPDATE_BROKER,
        /**
         * 队列变更(用于控制线程数)
         */
        QUEUE_CHANGE,
        /**
         * 读取集群信息
         */
        GET_CLUSTER,
        /**
         * 主题的队列数发生变化
         */
        TOPIC_QUEUE_CHANGE,

    }

}