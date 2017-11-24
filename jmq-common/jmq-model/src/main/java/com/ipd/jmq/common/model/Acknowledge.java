package com.ipd.jmq.common.model;

/**
 * 应答方式
 */
public enum Acknowledge {

    /**
     * 刷盘后应答
     */
    ACK_FLUSH(0),
    /**
     * 接收到数据应答
     */
    ACK_RECEIVE(1),
    /**
     * 不应答
     */
    ACK_NO(2);

    private int value;

    Acknowledge(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static Acknowledge valueOf(final int value) {
        switch (value) {
            case 0:
                return ACK_FLUSH;
            case 1:
                return ACK_RECEIVE;
            case 2:
                return ACK_NO;
            default:
                return ACK_FLUSH;
        }
    }
}
