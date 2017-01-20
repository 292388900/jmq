package com.ipd.jmq.server.broker.election;

import com.ipd.jmq.common.cluster.Broker;
import com.ipd.jmq.server.broker.BrokerConfig;
import com.ipd.jmq.server.broker.cluster.ClusterManager;
import com.ipd.jmq.toolkit.concurrent.EventListener;
import com.ipd.jmq.toolkit.lang.LifeCycle;
import com.ipd.jmq.toolkit.plugin.ServicePlugin;

/**
 * 主从选举
 *
 * @author tianya
 * @since 2014-01-21
 */
public interface RoleDecider extends LifeCycle, ServicePlugin {
    /**
     * 设置当前分组
     *
     * @param clusterManager 集群管理
     */
    void setClusterManager(ClusterManager clusterManager);

    /**
     * 设置配置数据
     *
     * @param brokerConfig 配置数据
     */
    void setBrokerConfig(BrokerConfig brokerConfig);

    /**
     * 增加监听器
     *
     * @param listener
     */
    void addListener(EventListener<RoleEvent> listener);

    /**
     * 删除监听器
     *
     * @param listener
     */
    void removeListener(EventListener<RoleEvent> listener);

    /**
     * 释放主节点，并恢复原来的主节点为指定节点
     *
     * @param lastMaster 以前的主节点
     */
    void releaseMaster(Broker lastMaster);

}