package com.ipd.jmq.server.broker.monitor.impl;

import com.alibaba.fastjson.JSON;
import com.ipd.jmq.server.broker.monitor.api.TopicMonitor;
import com.ipd.jmq.common.monitor.MetricsInfo;
import com.ipd.jmq.common.monitor.RetryPerfBuffer;
import com.ipd.jmq.common.monitor.RetryStat;
import com.ipd.jmq.server.broker.profile.PubSubStat;
import com.ipd.jmq.toolkit.stat.TPStat;
import com.ipd.jmq.toolkit.stat.TPStatBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by zhangkepeng on 16-11-30.
 */
public class TopicMonitorImpl implements TopicMonitor {

    private static final Logger logger = LoggerFactory.getLogger(TopicMonitorImpl.class);

    // 客户端性能统计
    protected PubSubStat pubSubStat;
    // 重试性能统计
    protected RetryPerfBuffer retryPerfBuffer;

    public TopicMonitorImpl(Builder builder) {
        this.pubSubStat = builder.pubSubStat;
        this.retryPerfBuffer = builder.retryPerfBuffer;
    }

    @Override
    public void addTopic(String topic, short queueCount) {
        // TODO
    }

    @Override
    // 获取客户端性能指标
    public String collectMetrics() {
        PubSubStat.PubSubStatSlice slice = pubSubStat.slice();
        ConcurrentMap<String, TPStatBuffer> consumeStats = slice.getStats(PubSubStat.StatType.consume);
        ConcurrentMap<String, TPStatBuffer> produceStats = slice.getStats(PubSubStat.StatType.produce);
        List<MetricsInfo> result = new ArrayList<MetricsInfo>(consumeStats != null ? consumeStats.size() : 0 +
                (produceStats != null ? produceStats.size() : 0) + 5);

        result.addAll(convertToMetricInfoJson(MetricsInfo.PRODUCER, produceStats));
        result.addAll(convertToMetricInfoJson(MetricsInfo.CONSUMER, consumeStats));

        return JSON.toJSONString(result);

    }

    private List<MetricsInfo> convertToMetricInfoJson(String role, ConcurrentMap<String, TPStatBuffer> stats) {

        List<MetricsInfo> result = new ArrayList<MetricsInfo>(stats.size() + 5);
        if (stats.size() == 0) {
            return result;
        }

        for (Map.Entry<String, TPStatBuffer> entry : stats.entrySet()) {
            String key = entry.getKey();
            TPStatBuffer buffer = entry.getValue();
            if (buffer == null || buffer.getTPStat() == null) {
                logger.warn("No correlation stat,key=" + key);
                continue;
            }

            String[] parts = key.split(":");
            String topic = parts[0];
            String app = parts[1];

            MetricsInfo metricsInfo = new MetricsInfo();
            metricsInfo.setTopic(topic);
            metricsInfo.setApp(app);
            metricsInfo.setRole(role);

            TPStat stat = buffer.getTPStat();
            metricsInfo.setpMax(stat.getMax());
            metricsInfo.setpTp50(stat.getTp50());
            metricsInfo.setpTp99(stat.getTp99());
            metricsInfo.setpTps(stat.getTps());


            if (metricsInfo.isConsumer()) {
                /**
                 *重试统计
                 */
                RetryStat retryStat = retryPerfBuffer.getStat(topic, app);
                if (retryStat != null) {
                    metricsInfo.setAddRetry(retryStat.getAddRetry().get());
                    metricsInfo.setRetrySuccess(retryStat.getRetrySuccess().get());
                    metricsInfo.setRetryError(retryStat.getRetryError().get());
                }
            }

            result.add(metricsInfo);
        }

        return result;
    }

    public static class Builder {

        // 客户端性能统计
        protected PubSubStat pubSubStat;
        // 重试性能统计
        protected RetryPerfBuffer retryPerfBuffer;

        public Builder(PubSubStat pubSubStat, RetryPerfBuffer retryPerfBuffer) {
            this.pubSubStat = pubSubStat;
            this.retryPerfBuffer = retryPerfBuffer;
        }

        public TopicMonitor build() {
            return new TopicMonitorImpl(this);
        }
    }
}
