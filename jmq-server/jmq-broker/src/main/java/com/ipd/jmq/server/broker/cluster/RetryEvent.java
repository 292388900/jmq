package com.ipd.jmq.server.broker.cluster;

import java.util.Set;

/**
 * 队列变更事件
 */
public class RetryEvent extends ClusterEvent {
    // 连接地址
    private Set<String> addresses;

    public RetryEvent(Set<String> addresses) {
        this.type = EventType.RETRY_CHANGE;
        this.addresses = addresses;
    }

    public Set<String> getAddresses() {
        return addresses;
    }
}
