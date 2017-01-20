package com.ipd.jmq.registry.listener;

import com.ipd.jmq.toolkit.concurrent.EventListener;

/**
 * 集群选举监听器
 */
public interface ClusterListener extends EventListener<ClusterEvent> {

    /**
     * 返回当前节点名称
     *
     * @return 节点名称
     */
    String getIdentity();

}
