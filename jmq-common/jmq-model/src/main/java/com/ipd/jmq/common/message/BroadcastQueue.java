package com.ipd.jmq.common.message;

/**
 * Created by zhangkepeng on 15-9-6.
 */
public class BroadcastQueue {
    // 分组名称
    protected String group;
    // 主题
    protected String topic;
    // 应用
    protected String app;
    // 队列ID(>=1)
    protected short queueId;

    public BroadcastQueue() {

    }

    public BroadcastQueue(String group, String topic, String app, short queueId) {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BroadcastQueue that = (BroadcastQueue) o;

        if (group != null ? !group.equals(that.group) : that.group != null) {
            return false;
        }
        if (topic != null ? !topic.equals(that.topic) : that.topic != null) {
            return false;
        }
        if (app != null ? !app.equals(that.app) : that.app != null) {
            return false;
        }
        if (queueId != that.queueId) {
            return false;
        }

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

}
