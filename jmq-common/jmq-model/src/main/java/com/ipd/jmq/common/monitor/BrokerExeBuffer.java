package com.ipd.jmq.common.monitor;


import com.ipd.jmq.toolkit.stat.TPStatDoubleBuffer;

/**
 * Broker异常统计双缓冲区
 */
public class BrokerExeBuffer extends TPStatDoubleBuffer<BrokerExe> {

    public BrokerExeBuffer(String name) {
        super(new BrokerExe(name), new BrokerExe(name));
    }

    public BrokerExeBuffer(String name, String group) {
        super(new BrokerExe(name, group), new BrokerExe(name, group));
    }

    /**
     * 移除消费者
     *
     * @param app   应用
     * @param topic 主题
     */
    public void removeConsumer(String app, String topic) {
        lock.readLock().lock();
        try {
            AppExceptionStat appExceptionStat = writeStat.getAndCreateAppExceptionStat(app);
            if (appExceptionStat != null) {
                TopicExceptionStat topicExceptionStat = appExceptionStat.getAndCreateTopicExceptionStat(topic);
                if (topicExceptionStat != null) {
                    topicExceptionStat.getConsumerRemoveTimes().incrementAndGet();
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 移除生产者
     *
     * @param app   应用
     * @param topic 主题
     */

    public void removeProducer(String app, String topic) {
        lock.readLock().lock();
        try {
            AppExceptionStat appExceptionStat = writeStat.getAndCreateAppExceptionStat(app);
            if (appExceptionStat != null) {
                TopicExceptionStat topicExceptionStat = appExceptionStat.getAndCreateTopicExceptionStat(topic);
                if (topicExceptionStat != null) {
                    topicExceptionStat.getProducerRemoveTimes().incrementAndGet();
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 移除连接
     *
     * @param app 应用
     */
    public void removeConnection(String app) {
        lock.readLock().lock();
        try {
            AppExceptionStat appExceptionStat = writeStat.getAndCreateAppExceptionStat(app);
            if (appExceptionStat != null) {
                appExceptionStat.getDisconnectTimes().incrementAndGet();
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 发送失败
     */
    public void onSendFailure() {
        lock.readLock().lock();
        try {
            writeStat.getSendFailureTimes().incrementAndGet();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 清理过期消息
     *
     * @param app   应用
     * @param topic 主题
     * @param count 过期次数
     */
    public void onCleanExpire(String app, String topic, int count) {
        lock.readLock().lock();
        try {
            AppExceptionStat appExceptionStat = writeStat.getAndCreateAppExceptionStat(app);
            if (appExceptionStat != null) {
                TopicExceptionStat topicExceptionStat = appExceptionStat.getAndCreateTopicExceptionStat(topic);
                if (topicExceptionStat != null) {
                    topicExceptionStat.getConsumeExpireTimes().addAndGet(count);
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }
}
