package com.ipd.jmq.replication;

/**
 * Created by guoliang5 on 2016/8/23.
 */
public class ReplicaEvent {
    public ReplicaEvent(State state, long waterMark, Replica replica) {
        this.state = state;
        this.waterMark = waterMark;
        this.replica = replica;
    }

    public static enum State {
        esInsync,                // 已经同步了，可以通过syncOffset得到同步位置
        esSynchronizing,        // 正在同步中，可以通过syncOffset得到同步位置
        esAddReplica,
        esRemoveReplica
    }

    public State state;
    public long waterMark;
    public Replica replica;
}
