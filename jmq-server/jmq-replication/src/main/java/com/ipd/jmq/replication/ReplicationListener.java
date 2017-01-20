package com.ipd.jmq.replication;

import com.ipd.jmq.common.cluster.ClusterRole;

/**
 * 复制系统事件监听器
 */
public interface ReplicationListener {

    /**
     * 复制服务启动事件
     *
     * @throws Exception
     */
    void onStart() throws Exception;

    /**
     * 复制服务关闭事件
     */
    void onStop();

    /**
     * 复制服务启动事件
     *
     * @param role
     * @throws Exception
     */
    void onStart(ClusterRole role) throws Exception;

    /**
     * replica添加事件
     *
     * @param replica
     */
    void onAddReplica(Replica replica);

    /**
     * replica移除事件
     *
     * @param replica
     */
    void onRemoveReplica(Replica replica);


}
