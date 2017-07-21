package com.ipd.jmq.common.network.kafka.command;

/**
 * Created by zhangkepeng on 16-7-28.
 */
public class Request {

    public static final int ordinaryConsumerId = -1;
    public static final int debuggingConsumerId = -2;

    public static boolean isValidBrokerId(int brokerId) {
        return brokerId >= 0;
    }
}
