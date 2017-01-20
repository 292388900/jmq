package com.ipd.jmq.replication;

import com.ipd.jmq.toolkit.concurrent.EventListener;

import java.util.List;

/**
 * Created by guoliang5 on 2016/8/16.
 */
public  interface ReplicationMaster {
    /**
     * 获取当前水位
     * @return 返回当前水位
     */
    long getWaterMark();

    /**
     * 获取主从同步状态
     * @return true=目前处于同步状态
     */
    boolean isInsync();

    /**
     * 获取所有复制者
     * @return 复制者列表
     */
    List<Replica> getAllReplicas();

    /**
     * 获取处于同步状态的复制者列表
     * @return 当前处于同步状态的复制者列表
     */
    List<Replica> getInsyncReplicas();

    /**
     * 添加监听者
     * @param listener
     */
    void addListener(EventListener<ReplicaEvent> listener);

    /**
     * 移除监听者
     * @param listener
     */
    void removeListener(EventListener<ReplicaEvent> listener);

    /**
     * 获取复制统计信息
     * @return
     */
    RepStat getRepStat();
}
