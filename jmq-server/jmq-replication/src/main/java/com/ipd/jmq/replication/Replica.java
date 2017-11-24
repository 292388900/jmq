package com.ipd.jmq.replication;

import com.ipd.jmq.common.cluster.Broker;
import com.ipd.jmq.common.network.Transport;

import java.net.SocketAddress;

/**
 * Created by guoliang5 on 2016/8/16.
 */
public interface Replica {
    enum State {
        Insync,     // 当前处于同步状态
        Online,     // 当前处于在线状态
        Offline     // 当前处于离线状态
    }

    /**
     * 获取当前状态
     * @return
     */
    State getState();

    /**
     * 获取当前已复制位置
     * @return
     */
    long getReplicatedOffset();

    /**
     * 获取当前Broker描述对象
     * @return
     */
    Broker getBroker();

    /**
     * 获取Transport
     * @return
     */
    Transport getTransport();

    /**
     * 获取replica远程地址
     * @return
     */
    SocketAddress getAddress();

    /**
     * 关闭网络连接
     */
    void close();
}
