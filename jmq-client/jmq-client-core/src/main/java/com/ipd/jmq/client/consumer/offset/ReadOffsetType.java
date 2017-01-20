package com.ipd.jmq.client.consumer.offset;

/**
 * Created by zhangkepeng on 15-8-24.
 */
public enum ReadOffsetType {
    /**
     * From memory
     */
    READ_FROM_MEMORY,
    /**
     * From storage
     */
    READ_FROM_STORE,
    /**
     * From memory,then from storage
     */
    MEMORY_FIRST_THEN_STORE;
}
