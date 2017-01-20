package com.ipd.jmq.client.stat;

/**
 * 跟踪阶段
 */
public enum TracePhase {
    /**
     * 生产阶段
     */
    PRODUCE,
    /**
     * 接收数据阶段
     */
    RECEIVE,
    /**
     * 消费阶段
     */
    CONSUME,
    /**
     * 发送重试
     */
    SENDRETRY
}
