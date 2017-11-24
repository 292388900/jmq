package com.ipd.jmq.client.connection;

import com.ipd.jmq.client.cluster.ClusterEvent;
import com.ipd.jmq.client.cluster.ClusterManager;
import com.ipd.jmq.client.stat.Trace;
import com.ipd.jmq.common.cluster.BrokerGroup;
import com.ipd.jmq.common.cluster.Permission;
import com.ipd.jmq.toolkit.concurrent.EventListener;
import com.ipd.jmq.toolkit.lang.LifeCycle;

import java.util.List;

/**
 * 连接管理器
 */
public interface TransportManager extends LifeCycle {

    /**
     * 获取连接
     *
     * @param group      集群分组
     * @param topic      主题
     * @param permission 读写权限，区分生产者和消费者
     * @return 连接
     */
    GroupClient getTransport(BrokerGroup group, String topic, Permission permission);

    /**
     * 移除连接
     *
     * @param group      集群分组
     * @param topic      主题
     * @param permission 读写权限，区分生产者和消费者
     */
    void removeTransport(BrokerGroup group, String topic, Permission permission);

    /**
     * 获取连接
     *
     * @param topic      主题
     * @param permission 读写权限，区分生产者和消费者
     * @return 连接列表
     */
    List<GroupTransport> getTransports(String topic, Permission permission);

    /**
     * 移除连接
     *
     * @param topic      主题
     * @param permission 读写权限，区分生产者和消费者
     */
    void removeTransports(String topic, Permission permission);

    /**
     * 传输连接配置信息
     *
     * @return 连接配置
     */
    TransportConfig getConfig();

    /**
     * 获取集群管理器
     *
     * @return 集群管理器
     */
    ClusterManager getClusterManager();

    /**
     * 获取性能跟踪器
     *
     * @return 性能跟踪器
     */
    Trace getTrace();

    /**
     * 添加消费者集群变化监听
     *
     * @param topic    主题
     * @param listener 监听器
     */
    void addListener(String topic, EventListener<ClusterEvent> listener);

    /**
     * 移除消费者集群监听器
     *
     * @param topic    主题
     * @param listener 监听器
     */
    void removeListener(String topic, EventListener<ClusterEvent> listener);

}