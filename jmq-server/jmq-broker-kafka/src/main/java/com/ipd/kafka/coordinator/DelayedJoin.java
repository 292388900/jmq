package com.ipd.kafka.coordinator;

import com.ipd.kafka.DelayedOperation;

/**
 * Created by zhangkepeng on 17-2-13.
 */
public class DelayedJoin extends DelayedOperation {

    private GroupCoordinator coordinator;
    private GroupMetadata group;
    private long sessionTimeout;

    public DelayedJoin(GroupCoordinator coordinator, GroupMetadata group, long sessionTimeout) {
        super(sessionTimeout);
        this.coordinator = coordinator;
        this.group = group;
        this.sessionTimeout = sessionTimeout;
    }

    @Override
    protected boolean tryComplete() {
        return coordinator.tryCompleteJoin(group, this);
    }

    @Override
    protected void onExpiration() {
        coordinator.onExpireJoin();
    }

    @Override
    protected void onComplete() {
        coordinator.onCompleteJoin(group);
    }
}
