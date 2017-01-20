package com.ipd.jmq.common.monitor;

import com.ipd.jmq.toolkit.stat.TPStatSlice;
import com.ipd.jmq.toolkit.time.MilliPeriod;
import com.ipd.jmq.toolkit.time.Period;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Retry统计
 *
 * @author bjllw
 */
public class RetryPerf implements TPStatSlice {
    private ConcurrentHashMap<String, RetryStat> retryMap = new ConcurrentHashMap<String, RetryStat>();
    private final String separator = "##";
    private MilliPeriod period = new MilliPeriod();

    @Override
    public Period getPeriod() {
        return period;
    }

    @Override
    public void clear() {

        Collection<RetryStat> states = retryMap.values();
        if (states.size() > 0) {
            for (RetryStat stat : states) {
                stat.reset();
            }
        }

    }

    public RetryStat getRetryStat(String topic, String app) {

        String key = topic + separator + app;

        return retryMap.get(key);
    }


    public RetryStat getAndCreateRetyStat(String topic, String app) {


        String key = topic + separator + app;

        RetryStat retryStat = retryMap.get(key);
        if (retryStat != null) {
            return retryStat;
        }

        retryStat = new RetryStat();
        retryStat.setTopic(topic);
        retryStat.setApp(app);

        RetryStat old = retryMap.put(key, retryStat);
        if (old != null) {
            retryStat = old;
        }


        return retryStat;
    }

}
