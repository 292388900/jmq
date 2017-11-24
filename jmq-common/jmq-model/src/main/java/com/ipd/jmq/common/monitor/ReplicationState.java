package com.ipd.jmq.common.monitor;

import java.io.Serializable;

/**
 * 主从复制级别
 *
 * @author xuzhenhua
 */
public class ReplicationState implements Serializable {

    private static final long serialVersionUID = -6860148151449204180L;
    // broker名称
    private String broker;
    // 从节点的在线数量
    private int onlineReplicas;
    // HAStore重启次数
    private int storeRestartTimes;

    public String getBroker() {
        return broker;
    }

    public void setBroker(String broker) {
        this.broker = broker;
    }

    public int getOnlineReplicas() {
        return onlineReplicas;
    }

    public void setOnlineReplicas(int onlineReplicas) {
        this.onlineReplicas = onlineReplicas;
    }

    public int getStoreRestartTimes() {
        return storeRestartTimes;
    }

    public void setStoreRestartTimes(int storeRestartTimes) {
        this.storeRestartTimes = storeRestartTimes;
    }
}
