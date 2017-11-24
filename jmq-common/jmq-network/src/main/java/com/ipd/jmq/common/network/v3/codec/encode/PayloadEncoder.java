package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.JMQPayload;
import com.ipd.jmq.common.network.Types;
import io.netty.buffer.ByteBuf;

/**
 * JMQ 命令实体编码器
 *
 * @author lindeqiang
 * @since 2016/8/14 13:35
 */
public interface PayloadEncoder<T extends JMQPayload> extends Types {
    /**
     * @param payload 实体
     * @param out     数据输出流
     * @throws Exception
     */
    void encode(final T payload, final ByteBuf out) throws Exception;
}
