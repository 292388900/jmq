package com.ipd.jmq.common.network.v3.command;

/**
 * 数据包方向
 * Created by hexiaofeng on 16-6-22.
 */
public enum Direction {
    /**
     * 请求
     */
    REQUEST(0),
    /**
     * 应答
     */
    RESPONSE(1);

    private int value;

    Direction(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static Direction valueOf(final int value) {
        switch (value) {
            case 0:
                return REQUEST;
            default:
                return RESPONSE;
        }
    }
}
