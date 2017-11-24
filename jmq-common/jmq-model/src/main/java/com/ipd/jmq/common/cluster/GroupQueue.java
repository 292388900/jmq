package com.ipd.jmq.common.cluster;

import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 分组队列信息.
 *
 * @author lindeqiang
 * @since 2016/9/4 14:19
 */
public class GroupQueue {
    // 主题
    private String topic;
    // Broker 分组
    private String group;
    // 队列
    private CopyOnWriteArrayList<Short> queues = new CopyOnWriteArrayList<>();

    public GroupQueue() {
    }

    public GroupQueue(String topic, String group) {
        this.topic = topic;
        this.group = group;
    }


    public void addQueue(Short queueId) {
        if (queues.contains(queueId)) {
            queues.add(queueId);
        }
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public CopyOnWriteArrayList<Short> getQueues() {
        return queues;
    }

    public void setQueues(CopyOnWriteArrayList<Short> queues) {
        this.queues = queues;
    }
}
