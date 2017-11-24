package com.ipd.jmq.common.monitor;

import java.io.Serializable;

/**
 * Created by wylixiaobin on 2016/12/16.
 */
public class CollectorMessage implements Serializable {
    private MessageMetric metric;
    private Object collectMsg;
    public CollectorMessage(){}
    public CollectorMessage(MessageMetric metric, Object collectMsg) {
        this.metric = metric;
        this.collectMsg = collectMsg;
    }

    public MessageMetric getMetric() {
        return metric;
    }

    public void setMetric(MessageMetric metric) {
        this.metric = metric;
    }

    public Object getCollectMsg() {
        return collectMsg;
    }

    public void setCollectMsg(Object collectMsg) {
        this.collectMsg = collectMsg;
    }
}
