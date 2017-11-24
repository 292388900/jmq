package com.ipd.jmq.common.network.v3.command;


/**
 * 命令类型.
 *
 * @author lindeqiang
 * @since 2016/7/20 10:31
 */
public final class CmdTypes {
    // 发送消息
    public static final int PUT_MESSAGE = 1;
    // 取消息
    public static final int GET_MESSAGE = 2;
    // 取消息应答
    public static final int GET_MESSAGE_ACK = 102;
    // 消费应答消息
    public static final int ACK_MESSAGE = 3;
    // 重试消息
    public static final int RETRY_MESSAGE = 4;
    // 存放重试消息
    public static final int PUT_RETRY = 5;
    // 获取重试消息
    public static final int GET_RETRY = 6;
    // 获取重试消息应答
    public static final int GET_RETRY_ACK = 106;
    // 更新重试消息
    public static final int UPDATE_RETRY = 7;
    // 获取重试条数
    public static final int GET_RETRY_COUNT = 8;
    // 获取重试条数应答
    public static final int GET_RETRY_COUNT_ACK = 108;
    // 事务准备
    public static final int TX_PREPARE = 13;
    // 事务准备应答
    public static final int TX_PREPARE_ACK = 113;
    // 事务提交
    public static final int TX_COMMIT = 14;
    // 事务回滚
    public static final int TX_ROLLBACK = 15;
    // 事务查询
    public static final int TX_FEEDBACK = 16;
    // 事务查询应答
    public static final int TX_FEEDBACK_ACK = 116;
    // 消息 TODO 和 putMessage合并
    public static final int PUT_MESSAGE_RICH = 17;
    // 心跳
    public static final int HEARTBEAT = 30;
    // 获取集群
    public static final int GET_CLUSTER = 31;
    // 获取集群应答
    public static final int GET_CLUSTER_ACK = 131;
    // 获取生产健康状况
    public static final int GET_PRODUCER_HEALTH = 32;
    // 获取消费健康状况
    public static final int GET_CONSUMER_HEALTH = 37;
    // 增加连接
    public static final int ADD_CONNECTION = 33;
    // 删除连接
    public static final int REMOVE_CONNECTION = 133;
    // 增加生产者
    public static final int ADD_PRODUCER = 34;
    // 增加生产者
    public static final int REMOVE_PRODUCER = 134;
    // 删除消费者
    public static final int ADD_CONSUMER = 35;
    // 删除消费者
    public static final int REMOVE_CONSUMER = 135;
    // 客户端性能
    public static final int CLIENT_PROFILE = 36;
    // 客户端性能应答
    public static final int CLIENT_PROFILE_ACK = 136;
    // 复制身份
    public static final int IDENTITY = 50;
    // 获取复制偏移量
    public static final int GET_OFFSET = 51;
    // 获取复制偏移量应答
    public static final int GET_OFFSET_ACK = 151;
    // 获取复制日志
    public static final int GET_JOURNAL = 52;
    // 获取复制日志应答
    public static final int GET_JOURNAL_ACK = 152;
    // 投票选举master
    public static final int VOTE = 55;
    // 同步消费位置
    public static final int GET_CONSUMER_OFFSET = 58;
    // 同步消费位置确认
    public static final int GET_CONSUMER_OFFSET_ACK = 158;
    //下发Agent的系统指令
    public static final int SYSTEM_COMMAND = 83;
    // 布尔应答
    public static final int BOOLEAN_ACK = 100;
    // 重置消费位置
    public static final int RESET_ACK_OFFSET = 38;

}
