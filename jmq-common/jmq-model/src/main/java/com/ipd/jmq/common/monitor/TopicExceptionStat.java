package com.ipd.jmq.common.monitor;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 应用异常统计.
 *
 * @author lindeqiang
 * @since 2015/7/7 20:53
 */
public class TopicExceptionStat implements Serializable {
    //应用名
    private String topic;
    // 消费超时次数
    private AtomicInteger consumeExpireTimes = new AtomicInteger(0);
    // 生产者移除次数
    private AtomicInteger producerRemoveTimes = new AtomicInteger(0);
    //消费者移除次数
    private AtomicInteger consumerRemoveTimes = new AtomicInteger(0);

    public TopicExceptionStat(String topic) {
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public AtomicInteger getConsumeExpireTimes() {
        return consumeExpireTimes;
    }

    public void setConsumeExpireTimes(AtomicInteger consumeExpireTimes) {
        this.consumeExpireTimes = consumeExpireTimes;
    }

    public AtomicInteger getProducerRemoveTimes() {
        return producerRemoveTimes;
    }

    public void setProducerRemoveTimes(AtomicInteger producerRemoveTimes) {
        this.producerRemoveTimes = producerRemoveTimes;
    }

    public AtomicInteger getConsumerRemoveTimes() {
        return consumerRemoveTimes;
    }

    public void setConsumerRemoveTimes(AtomicInteger consumerRemoveTimes) {
        this.consumerRemoveTimes = consumerRemoveTimes;
    }

    public void clear() {
        consumeExpireTimes.set(0);
        producerRemoveTimes.set(0);
        consumerRemoveTimes.set(0);
    }
}
