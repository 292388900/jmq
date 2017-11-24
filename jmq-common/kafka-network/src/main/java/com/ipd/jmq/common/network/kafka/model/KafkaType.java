package com.ipd.jmq.common.network.kafka.model;

/**
 * Created by zhangkepeng on 16-7-27.
 */
public enum KafkaType {

    PRODUCE(0, "produce"),
    FETCH(1, "fetch"),
    LIST_OFFSETS(2, "list_offsets"),
    METADATA(3, "metadata"),
    LEADER_AND_ISR(4, "leader_and_isr"),
    STOP_REPLICA(5, "stop_replica"),
    OFFSET_COMMIT(8, "offset_commit"),
    OFFSET_FETCH(9, "offset_fetch"),
    CONSUMER_METADATA(10, "consumer_metadata"),
    JOIN_GROUP(11, "join_group"),
    HEARTBEAT(12, "heartbeat");

    /** the perminant and immutable id of an API--this can't change ever */
    public final short id;

    /** an english description of the api--this is for debugging and can change */
    public final String name;

    private KafkaType(int id, String name) {
        this.id = (short) id;
        this.name = name;
    }
}
