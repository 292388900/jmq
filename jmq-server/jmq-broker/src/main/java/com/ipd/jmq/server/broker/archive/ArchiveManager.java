package com.ipd.jmq.server.broker.archive;

import com.ipd.jmq.common.message.BrokerMessage;
import com.ipd.jmq.common.message.MessageLocation;
import com.ipd.jmq.common.network.v3.session.Connection;

import java.nio.ByteBuffer;

/**
 * Created by lihonglin1 on 2017/2/9.
 */
public interface ArchiveManager {
    /**
     * 写生产者归档文件
     *
     * @param message 消息
     */
    void writeProduce(final BrokerMessage message);

    /**
     * 写消费者归档文件
     *
     * @param connection 连接
     * @param location   消费位置
     */
    void writeConsume(final Connection connection, final MessageLocation location);

    /**
     * 写消费者归档文件
     *
     * @param location 消费位置
     */
    void writeConsume(final String app, final String clientIp, final MessageLocation location);

    /**
     * 生产归档记录刷盘到文件，满足数据条数或到达时间点都会触发
     *
     * @param targets 生产归档记录
     * @param buffer  缓冲区
     * @throws Exception
     */
     void flushProduce(Object[] targets, ByteBuffer buffer) throws Exception;

    /**
     * 消费者归档文件刷盘
     *
     * @param targets 消费归档记录
     * @param buffer  缓冲区
     * @throws Exception
     */
    void flushConsume(Object[] targets, ByteBuffer buffer) throws Exception;
}
