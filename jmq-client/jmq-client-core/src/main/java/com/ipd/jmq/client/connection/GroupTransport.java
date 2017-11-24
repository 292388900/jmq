package com.ipd.jmq.client.connection;

import com.ipd.jmq.common.cluster.BrokerGroup;
import com.ipd.jmq.common.network.v3.netty.failover.ElectTransport;
import com.ipd.jmq.common.network.v3.session.ConnectionId;
import com.ipd.jmq.common.network.FailoverTransport;

/**
 * 客户端传输通道
 *
 * @author lindeqiang
 * @since 14-4-29 上午10:37
 */

public interface GroupTransport extends FailoverTransport, ElectTransport {

    /**
     * 获取连接ID
     *
     * @return 连接ID
     */
    ConnectionId getConnectionId();

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