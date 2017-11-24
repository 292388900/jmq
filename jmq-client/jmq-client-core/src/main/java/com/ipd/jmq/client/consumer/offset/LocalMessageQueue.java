package com.ipd.jmq.client.consumer.offset;

/**
 * Created by dingjun on 15-9-16.
 */
public class LocalMessageQueue {
    // 分组名称
    protected String group;
    // 主题
    protected String topic;
    // 应用
    protected String app;
    // 队列ID(>=1)
    protected short queueId;
    // 入队时间戳
    protected String time;

    private LocalMessageQueue() {

    }

    public LocalMessageQueue(String group, String topic, String app, short queueId) {
        if (queueId < 1) {
            throw new IllegalArgumentException("queueId is invalid");
        }
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("topic is invalid");
        }
        if (group == null || group.isEmpty()) {
            throw new IllegalArgumentException("group is invalid");
        }
        if (app == null || app.isEmpty()) {
            throw new IllegalArgumentException("app is invalid");
        }
        this.group = group;
        this.topic = topic;
        this.app = app;
        this.queueId = queueId;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getGroup() {
        return group;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public void setQueueId(short queueId) {
        this.queueId = queueId;
    }

    public short getQueueId() {
        return queueId;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LocalMessageQueue that = (LocalMessageQueue) o;

        if (queueId != that.queueId) return false;
        if (!app.equals(that.app)) return false;
        if (!group.equals(that.group)) return false;
        if (!topic.equals(that.topic)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = group != null ? group.hashCode() : 0;
        result = 31 * result + (topic != null ? topic.hashCode() : 0);
        result = 31 * result + (app != null ? app.hashCode() : 0);
        result = 31 * result + (int) queueId;
        return result;
    }

    @Override
    public String toString() {
        return "LocalMessageQueue{" +
                "group='" + group + '\'' +
                ", topic='" + topic + '\'' +
                ", app='" + app + '\'' +
                ", queueId=" + queueId +
                '}';
    }
}
