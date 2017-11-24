package com.ipd.jmq.common.network;

/**
 * 通道事件
 */
public class TransportEvent {

    // 类型
    private EventType type;
    // 通道
    private Transport transport;

    /**
     * @param type
     * @param transport
     */
    public TransportEvent(EventType type, Transport transport) {
        this.type = type;
        this.transport = transport;
    }

    public EventType getType() {
        return this.type;
    }

    public Transport getTransport() {
        return this.transport;
    }

    /**
     * 事件类型
     */
    public enum EventType {
        /**
         * 连接
         */
        CONNECT,
        /**
         * 关闭
         */
        CLOSE,
        /**
         * 空闲
         */
        IDLE,
        /**
         * 异常
         */
        EXCEPTION

    }

}