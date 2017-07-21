package com.ipd.jmq.common.network.kafka.model;

import java.util.Set;

/**
 * Created by zhangkepeng on 16-8-10.
 *
 * 服务端缓存LeaderIsr
 */
public class LeaderIsr {

    private int leader;
    private int leaderEpoch;
    private Set<Integer> isr;
    private int zkVersion;

    public LeaderIsr(int leader, int leaderEpoch, Set<Integer> isr, int zkVersion) {
        this.leader = leader;
        this.leaderEpoch = leaderEpoch;
        this.isr = isr;
        this.zkVersion = zkVersion;
    }

    public int getZkVersion() {
        return zkVersion;
    }

    public void setZkVersion(int zkVersion) {
        this.zkVersion = zkVersion;
    }

    public Set<Integer> getIsr() {
        return isr;
    }

    public void setIsr(Set<Integer> isr) {
        this.isr = isr;
    }

    public int getLeaderEpoch() {
        return leaderEpoch;
    }

    public void setLeaderEpoch(int leaderEpoch) {
        this.leaderEpoch = leaderEpoch;
    }

    public int getLeader() {
        return leader;
    }

    public void setLeader(int leader) {
        this.leader = leader;
    }
}
