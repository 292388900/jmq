package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.network.v3.command.JMQPayload;
import com.ipd.jmq.common.network.Types;
import io.netty.buffer.ByteBuf;

/**
 * JMQ 命令实体解码器
 *
 * @author lindeqiang
 * @since 2016/8/14 13:35
 */
public interface PayloadDecoder<T extends JMQPayload> extends Types {
    /**
     * @param payload 实体
     * @param in      数据输入流
     * @return
     * @throws Exception
     */
    T decode(final T payload, final ByteBuf in) throws Exception;
}
