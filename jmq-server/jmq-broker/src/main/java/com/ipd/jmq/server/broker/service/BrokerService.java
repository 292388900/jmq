package com.ipd.jmq.server.broker.service;

import com.ipd.jmq.server.broker.BrokerConfig;
import com.ipd.jmq.server.broker.SessionManager;
import com.ipd.jmq.server.broker.cluster.ClusterManager;
import com.ipd.jmq.server.broker.dispatch.DispatchService;
import  com.ipd.jmq.server.broker.election.RoleDecider;
import com.ipd.jmq.server.broker.handler.DefaultHandlerFactory;
import com.ipd.jmq.server.store.Store;
import com.ipd.jmq.toolkit.concurrent.Scheduler;
import com.ipd.jmq.toolkit.lang.LifeCycle;
import com.ipd.jmq.toolkit.plugin.ServicePlugin;

/**
 * Created by zhangkepeng on 16-12-15.
 */
public interface BrokerService extends LifeCycle, ServicePlugin {

    /**
     * 设置命令处理工厂类
     *
     * @param factory
     */
    void setCommandHandlerFactory(DefaultHandlerFactory factory);

    /**
     * 设置分片配置
     *
     * @param config 集群管理器
     */
    void setBrokerConfig(BrokerConfig config);

    /**
     * 设置存储实现
     *
     * @param store 存储实现
     */
    void setStore(Store store);

    /**
     * 设置集群管理器
     *
     * @param clusterManager 集群管理器
     */
    void setClusterManager(ClusterManager clusterManager);

    /**
     * 设置会话管理
     *
     * @param sessionManager 会话管理
     */
    void setSessionManager(SessionManager sessionManager);

    /**
     * 共用调度线程池
     * @param scheduler
     */
    void setScheduler(Scheduler scheduler);

    /**
     * 设置角色管理器
     *
     * @param roleDecider 角色管理器
     */
    void setRoleDecider(RoleDecider roleDecider);

    /**
     * 设置分发服务
     *
     * @param dispatchService
     */
    void setDispatchService(DispatchService dispatchService);
}
