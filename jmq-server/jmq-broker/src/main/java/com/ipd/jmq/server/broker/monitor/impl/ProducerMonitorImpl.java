package com.ipd.jmq.server.broker.monitor.impl;

import com.ipd.jmq.server.broker.monitor.api.ProducerMonitor;

/**
 * Created by zhangkepeng on 16-11-30.
 */
public class ProducerMonitorImpl implements ProducerMonitor{

    public ProducerMonitorImpl(Builder builder) {

    }

    @Override
    public void sendMessage(String topic, String app, String body, String businessId) {
        // TODO
    }

    public static class Builder {

        public ProducerMonitor build() {
            return new ProducerMonitorImpl(this);
        }
    }
}
