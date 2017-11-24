package com.ipd.jmq.common.network.kafka.model;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by zhangkepeng on 16-8-10.
 *
 * zookeeper节点更新序列化类
 */
public class TopicState implements Serializable{

    private int controller_epoch = 0;
    private int leader;
    private int version = 1;
    private int leader_epoch = 0;
    private Set<Integer> isr = new HashSet<Integer>();

    public TopicState() {

    }

    public TopicState(int leader, Set<Integer> isr) {
        this(0, 0, leader, isr);
    }

    public TopicState(int leaderEpoch, int controllerEpoch, int leader, Set<Integer> isr) {
        this.leader_epoch = leaderEpoch;
        this.controller_epoch = controllerEpoch;
        this.leader = leader;
        this.isr = isr;
    }

    public int getController_epoch() {
        return controller_epoch;
    }

    public void setController_epoch(int controller_epoch) {
        this.controller_epoch = controller_epoch;
    }

    public int getLeader() {
        return leader;
    }

    public void setLeader(int leader) {
        this.leader = leader;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getLeader_epoch() {
        return leader_epoch;
    }

    public void setLeader_epoch(int leader_epoch) {
        this.leader_epoch = leader_epoch;
    }

    public Set<Integer> getIsr() {
        return isr;
    }

    public void setIsr(Set<Integer> isr) {
        this.isr = isr;
    }
}
