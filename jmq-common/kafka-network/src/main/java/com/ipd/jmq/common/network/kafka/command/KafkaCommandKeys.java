package com.ipd.jmq.common.network.kafka.command;

/**
 * Created by zhangkepeng on 16-8-1.
 */
public final class KafkaCommandKeys {

    public static final short BASE = 10000;

    // 发送消息
    public static final short PRODUCE = BASE + 0;
    // 取消息
    public static final short FETCH = BASE + 1;
    // 取offsets
    public static final short LIST_OFFSETS = BASE + 2;
    // 取元数据
    public static final short METADATA = BASE + 3;
    // 取leader和isr
    public static final short LEADER_AND_ISR = BASE + 4;
    // 停掉复制
    public static final short STOP_REPLICA = BASE + 5;
    // 更新元数据
    public static final short UPDATE_METADATA = BASE + 6;
    // 提交commit
    public static final short OFFSET_COMMIT = BASE + 8;
    // 取offset
    public static final short OFFSET_FETCH = BASE + 9;
    // 取消费者元数据
    public static final short GROUP_COORDINATOR = BASE + 10;
    // 加入组
    public static final short JOIN_GROUP = BASE + 11;
    // 心跳
    public static final short HEARTBEAT = BASE + 12;
    // 离开组
    public static final short LEAVE_GROUP = BASE + 13;
    // 同步组
    public static final short SYNC_GROUP = BASE + 14;

    // 自定义命令
    public static final short UPDATE_TOPICS_BROKER = BASE + 101;
    public static final short OFFSET_QUERY = BASE + 102;

}
