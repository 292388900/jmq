package com.ipd.jmq.common.network.kafka.model;

/**
 * Created by zhangkepeng on 17-2-13.
 */
public enum GroupState {

    /**
     * Group is preparing to rebalance
     *
     * action: respond to heartbeats with REBALANCE_IN_PROGRESS
     *         respond to sync group with REBALANCE_IN_PROGRESS
     *         remove member on leave group request
     *         park join group requests from new or existing members until all expected members have joined
     *         allow offset commits from previous generation
     *         allow offset fetch requests
     * transition: some members have joined by the timeout => AwaitingSync
     *             all members have left the group => Dead
     */
    PREPARINGREBALANCE((byte)1),
    /**
     * Group is awaiting state assignment from the leader
     *
     * action: respond to heartbeats with REBALANCE_IN_PROGRESS
     *         respond to offset commits with REBALANCE_IN_PROGRESS
     *         park sync group requests from followers until transition to Stable
     *         allow offset fetch requests
     * transition: sync group with state assignment received from leader => Stable
     *             join group from new member or existing member with updated metadata => PreparingRebalance
     *             leave group from existing member => PreparingRebalance
     *             member failure detected => PreparingRebalance
     */
    AWAITINGSYNC((byte)2),
    /**
     * Group is stable
     *
     * action: respond to member heartbeats normally
     *         respond to sync group from any member with current assignment
     *         respond to join group from followers with matching metadata with current group metadata
     *         allow offset commits from member of current generation
     *         allow offset fetch requests
     * transition: member failure detected via heartbeat => PreparingRebalance
     *             leave group from existing member => PreparingRebalance
     *             leader join-group received => PreparingRebalance
     *             follower join-group with new metadata => PreparingRebalance
     */
    STABLE((byte)3),
    /**
     * Group has no more members
     *
     * action: respond to join group with UNKNOWN_MEMBER_ID
     *         respond to sync group with UNKNOWN_MEMBER_ID
     *         respond to heartbeat with UNKNOWN_MEMBER_ID
     *         respond to leave group with UNKNOWN_MEMBER_ID
     *         respond to offset commit with UNKNOWN_MEMBER_ID
     *         allow offset fetch requests
     * transition: Dead is a final state before group metadata is cleaned up, so there are no transitions
     */
    DEAD((byte)4);

    private byte state;

    public byte getState() {
        return state;
    }

    GroupState(byte state) {
        this.state = state;
    }
}
