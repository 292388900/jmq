package com.ipd.jmq.common.lb;

/**
 * ClientType
 *
 * @author luoruiheng
 * @since 8/4/16
 */
public enum  ClientType {

    /**
     * 消费者
     */
    CONSUMER,

    /**
     * 生产者
     */
    PRODUCER;


    public static ClientType valueOf(int value) {
        switch (value) {
            case 2:
                return PRODUCER;
            case 1:
            default:
                return CONSUMER;
        }
    }


}
