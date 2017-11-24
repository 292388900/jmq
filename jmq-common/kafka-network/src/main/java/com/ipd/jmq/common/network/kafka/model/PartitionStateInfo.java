package com.ipd.jmq.common.network.kafka.model;

import java.util.Set;

/**
 * Created by zhangkepeng on 16-8-8.
 *
 * 服务端partition状态信息缓存
 */
public class PartitionStateInfo {

    private LeaderIsr leaderIsr;
    private Set<Integer> replicas;

    public LeaderIsr getLeaderIsr() {
        return leaderIsr;
    }

    public void setLeaderIsr(LeaderIsr leaderIsr) {
        this.leaderIsr = leaderIsr;
    }

    public Set<Integer> getReplicas() {
        return replicas;
    }

    public void setReplicas(Set<Integer> replicas) {
        this.replicas = replicas;
    }
}

