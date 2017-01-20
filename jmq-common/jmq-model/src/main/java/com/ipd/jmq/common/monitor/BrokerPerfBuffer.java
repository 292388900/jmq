package com.ipd.jmq.common.monitor;


import com.ipd.jmq.toolkit.stat.TPStatDoubleBuffer;

/**
 * Broker性能统计双缓冲区
 */
public class BrokerPerfBuffer extends TPStatDoubleBuffer<BrokerPerf> {

    public BrokerPerfBuffer(String name) {
        super(new BrokerPerf(name), new BrokerPerf(name));
    }

    public BrokerPerfBuffer(String name, String group) {
        super(new BrokerPerf(name, group), new BrokerPerf(name, group));
    }

    /**
     * 增加生产统计
     *
     * @param topic 主题
     * @param app   应用
     * @param count 消息成功条数
     * @param size  消息大小
     * @param time  时间
     */
    public void putMessage(final String topic, final String app, final long count, final long size, final long time) {
        lock.readLock().lock();
        try {
            writeStat.getEnQueue().addAndGet(count);
            writeStat.getEnQueueSize().addAndGet(size);
            writeStat.getEnQueueTime().addAndGet(time);

            TopicPerf topicPerf = writeStat.getAndCreateTopicPerf(topic);
            topicPerf.getEnQueue().addAndGet(count);
            topicPerf.getEnQueueSize().addAndGet(size);
            topicPerf.getEnQueueTime().addAndGet(time);

            AppPerf appPerf = topicPerf.getAndCreateAppStat(app);
            appPerf.getEnQueue().addAndGet(count);
            appPerf.getEnQueueSize().addAndGet(size);
            appPerf.getEnQueueTime().addAndGet(time);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 增加消费统计
     *
     * @param topic 主题
     * @param app   应用
     * @param count 消息成功条数
     * @param size  消息大小
     * @param time  时间
     */
    public void getMessage(final String topic, final String app, final long count, final long size, final long time) {
        lock.readLock().lock();
        try {
            writeStat.getDeQueue().addAndGet(count);
            writeStat.getDeQueueSize().addAndGet(size);
            writeStat.getDeQueueTime().addAndGet(time);

            TopicPerf topicPerf = writeStat.getAndCreateTopicPerf(topic);
            topicPerf.getDeQueue().addAndGet(count);
            topicPerf.getDeQueueSize().addAndGet(size);
            topicPerf.getDeQueueTime().addAndGet(time);

            AppPerf appPerf = topicPerf.getAndCreateAppStat(app);
            appPerf.getDeQueue().addAndGet(count);
            appPerf.getDeQueueSize().addAndGet(size);
            appPerf.getDeQueueTime().addAndGet(time);
        } finally {
            lock.readLock().unlock();
        }
    }
}
