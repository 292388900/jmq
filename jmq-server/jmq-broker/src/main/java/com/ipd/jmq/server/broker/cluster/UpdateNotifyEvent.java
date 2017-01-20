package com.ipd.jmq.server.broker.cluster;

/**
 * 主题修改通知时间
 */
public class UpdateNotifyEvent extends ClusterEvent {

    public UpdateNotifyEvent(EventType type) {
        this.type = type;
    }

}
