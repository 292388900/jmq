package com.ipd.jmq.common.network.v3.netty.failover;

import com.ipd.jmq.common.cluster.BrokerGroup;
import com.ipd.jmq.common.network.FailoverState;
import com.ipd.jmq.common.network.FailoverTransport;

/**
 * Created by dingjun on 15-10-27.
 */
public interface ElectTransport extends FailoverTransport {
    /**
     * 不健康状态
     */
    void weak();

    /**
     * 获取连接状态
     *
     * @return 连接状态
     */
    FailoverState getState();
    /**
     * 获取权重
     *
     * @return 权重
     */
    int getWeight();

    /**
     * 获取分组
     *
     * @return 分组
     */
    BrokerGroup getGroup();

    /**
     * 获取主题
     *
     * @return 主题
     */
    String getTopic();
}
