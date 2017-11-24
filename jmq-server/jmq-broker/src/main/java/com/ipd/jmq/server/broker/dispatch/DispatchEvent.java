package com.ipd.jmq.server.broker.dispatch;

/**
 * Created by lindeqiang on 2015/9/1.
 */
public class DispatchEvent {
    private String topic;
    private String app;
    private EventType type;

    public DispatchEvent(String topic, String app, EventType type) {
        this.topic = topic;
        this.app = app;
        this.type = type;
    }

    public EventType getType() {
        return this.type;
    }

    public static enum EventType {
        //无积压消息
        FREE,
        //有积压消息
        BACKLOG
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    @Override
    public String toString() {
        return "DispatchEvent{" +
                "topic='" + topic + '\'' +
                ", app='" + app + '\'' +
                ", type=" + type +
                '}';
    }
}
