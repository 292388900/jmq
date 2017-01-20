package com.ipd.jmq.common.model;

/**
 * 监控对象
 */
public class MonitorRecord {
    //监控指标
    private String metric;
    //机器
    private String host;
    // broker分组
    private String group;
    //消息实例名称
    private String broker;
    //队列名
    private String topic;
    //业务系统标识
    private String app;
    //采集时间，精确到秒
    private long time;

    private short field;

    private double value;

    public MonitorRecord() {
    }

    public MonitorRecord(String metric, String host, String broker, String topic, String app, double value) {
        this(metric, host, broker, topic, app, System.currentTimeMillis(), value);
    }

    public MonitorRecord(String metric, String host, String broker, String topic, String app, long time, double value) {
        this(metric, host, null, broker, topic, app, time, value);
    }

    public MonitorRecord(String metric, String host, String group, String broker, String topic, String app, long time,
                         double value) {
        this.metric = metric;
        this.host = host;
        this.broker = broker;
        this.topic = topic;
        this.app = app;
        this.time = time;
        this.group = group;
        this.value = value;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getBroker() {
        return broker;
    }

    public void setBroker(String broker) {
        this.broker = broker;
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

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public short getField() {
        return field;
    }

    public void setField(short field) {
        this.field = field;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("MonitorRecord{");
        sb.append("metric='").append(metric).append('\'');
        if (host != null && !host.isEmpty()) {
            sb.append(", host='").append(host).append('\'');
        }
        if (group != null && !group.isEmpty()) {
            sb.append(", group='").append(group).append('\'');
        }
        if (broker != null && !broker.isEmpty()) {
            sb.append(", broker='").append(broker).append('\'');
        }
        if (topic != null && !topic.isEmpty()) {
            sb.append(", topic='").append(topic).append('\'');
        }
        if (app != null && !app.isEmpty()) {
            sb.append(", app='").append(app).append('\'');
        }
        sb.append(", time=").append(time);
        sb.append(", value=").append(value);
        sb.append(", field=").append(field);
        sb.append('}');
        return sb.toString();
    }

}
