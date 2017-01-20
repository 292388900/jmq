package com.ipd.jmq.client.consumer;

/**
 * 消费者模式
 */
public enum ConsumerType {
    /**
     * 监听器模式
     */
    LISTENER,
    /**
     * 拉取模式，从缓冲区拉取
     */
    PULL,
    /**
     * 直接从服务器拉取
     */
    PULL_DIRECT
}