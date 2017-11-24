package com.ipd.jmq.client.stat;

import com.ipd.jmq.client.connection.TransportManager;
import com.ipd.jmq.common.network.v3.netty.NettyClient;
import com.ipd.jmq.toolkit.lang.LifeCycle;
import com.ipd.jmq.toolkit.plugin.ServicePlugin;

/**
 * 方法跟踪
 */
public interface Trace extends LifeCycle, ServicePlugin {

    /**
     * 设置通信客户端
     * @param client NettyClient
     */
    void setClient(NettyClient client);

    /**
     * set transport to get server brokers for ping
     * @param transportManager 连接管理器
     */
    void setTransportManager(TransportManager transportManager);

    /**
     * 获取应用代码
     *
     * @return 应用代码
     */
    String getApp();

    /**
     * 开始跟踪
     *
     * @param info 跟踪数据
     */
    void begin(TraceInfo info);

    /**
     * 结束跟踪
     *
     * @param info 跟踪数据
     */
    void end(TraceInfo info);
}
