package com.ipd.jmq.common.network;

/**
 * 故障切换连接状态
 */
public enum FailoverState {

    /**
     * 连接断开
     */
    DISCONNECTED,
    /**
     * 连上，健康
     */
    CONNECTED,
    /**
     * 连上，不健康
     */
    WEAK

}
