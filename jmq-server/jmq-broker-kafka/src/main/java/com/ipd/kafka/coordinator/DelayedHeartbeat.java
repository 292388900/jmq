package com.ipd.kafka.coordinator;

import com.ipd.kafka.DelayedOperation;

/**
 * Created by zhangkepeng on 17-2-13.
 */
public class DelayedHeartbeat extends DelayedOperation {

    private GroupCoordinator coordinator;
    private GroupMetadata group;
    private MemberMetadata member;
    private long heartbeatDeadline;
    private long sessionTimeout;

    public DelayedHeartbeat(GroupCoordinator coordinator, GroupMetadata group, MemberMetadata member, long heartbeatDeadline,
                            long sessionTimeout) {
        super(sessionTimeout);
        this.coordinator = coordinator;
        this.group = group;
        this.member = member;
        this.heartbeatDeadline = heartbeatDeadline;
        this.sessionTimeout = sessionTimeout;
    }

    @Override
    protected boolean tryComplete() {
        return coordinator.tryCompleteHeartbeat(group, member, heartbeatDeadline, this);
    }

    @Override
    protected void onExpiration() {
        coordinator.onExpireHeartbeat(group, member, heartbeatDeadline);
    }

    @Override
    protected void onComplete() {
        coordinator.onCompleteHeartbeat();
    }
}
