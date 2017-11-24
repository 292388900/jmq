package com.ipd.jmq.server.broker.cluster;

import com.ipd.jmq.common.cluster.BrokerGroup;

/**
 * Created with IntelliJ IDEA.
 *
 * @author lindeqiang
 * @since 2016/8/17 18:06
 */
public class BrokerGroupEvent extends ClusterEvent {
    private BrokerGroup group;

    public BrokerGroupEvent(BrokerGroup group) {
        this.type = EventType.ALL_BROKER_UPDATE;
        this.group = group;
    }

    public BrokerGroup getGroup() {
        return group;
    }

}
