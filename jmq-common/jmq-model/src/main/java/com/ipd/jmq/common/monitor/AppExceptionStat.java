package com.ipd.jmq.common.monitor;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 应用异常统计.
 *
 * @author lindeqiang
 * @since 2015/7/7 20:53
 */
public class AppExceptionStat implements Serializable {
    //应用名
    private String app;
    //断开连接次数
    private AtomicInteger disconnectTimes = new AtomicInteger(0);
    //主题异常统计
    private ConcurrentMap<String, TopicExceptionStat> topicExceptionStats = new ConcurrentHashMap<String, TopicExceptionStat>();

    public AppExceptionStat(String app) {
        this.app = app;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public AtomicInteger getDisconnectTimes() {
        return disconnectTimes;
    }

    public void setDisconnectTimes(AtomicInteger disconnectTimes) {
        this.disconnectTimes = disconnectTimes;
    }

    public ConcurrentMap<String, TopicExceptionStat> getTopicExceptionStats() {
        return topicExceptionStats;
    }

    public void setTopicExceptionStats(ConcurrentMap<String, TopicExceptionStat> topicExceptionStats) {
        this.topicExceptionStats = topicExceptionStats;
    }

    public TopicExceptionStat getAndCreateTopicExceptionStat(String topic) {
        TopicExceptionStat topicExceptionStat = topicExceptionStats.get(topic);
        if (topicExceptionStat == null) {
            topicExceptionStat = new TopicExceptionStat(topic);
            TopicExceptionStat old = topicExceptionStats.put(topic, topicExceptionStat);
            if (old != null) {
                topicExceptionStat = old;
            }
        }
        return topicExceptionStat;
    }

    public void clear() {
        disconnectTimes.set(0);
        for (Map.Entry<String, TopicExceptionStat> entry : topicExceptionStats.entrySet()) {
            entry.getValue().getConsumeExpireTimes().set(0);
            entry.getValue().getConsumerRemoveTimes().set(0);
            entry.getValue().getProducerRemoveTimes().set(0);
        }
    }
}
