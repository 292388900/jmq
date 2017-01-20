package com.ipd.jmq.common.monitor;

import com.ipd.jmq.toolkit.stat.TPStatDoubleBuffer;

/**
 * Broker性能统计双缓冲区
 */
public class RetryPerfBuffer extends TPStatDoubleBuffer<RetryPerf> {

    public RetryPerfBuffer() {
        super(new RetryPerf(), new RetryPerf());
    }


    public void onAddRetry(String topic, String app, long count) {
        writeStat.getAndCreateRetyStat(topic, app).onAddRetry(count);

    }

    public void onRetrySuccess(String topic, String app, long count) {
        writeStat.getAndCreateRetyStat(topic, app).onRetrySuccess(count);
    }


    public void onRetryError(String topic, String app, long count) {
        writeStat.getAndCreateRetyStat(topic, app).onRetryError(count);
    }

    public RetryStat getStat(String topic, String app) {
        return readStat.getRetryStat(topic, app);
    }
}
