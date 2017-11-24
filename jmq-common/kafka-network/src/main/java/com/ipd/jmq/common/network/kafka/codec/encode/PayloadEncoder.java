package com.ipd.jmq.common.network.kafka.codec.encode;

import com.ipd.jmq.common.network.v3.command.Payload;
import io.netty.buffer.ByteBuf;

/**
 * Created by zhangkepeng on 16-8-2.
 */
public interface PayloadEncoder<T extends Payload> {

    /**
     * @param payload 实体
     * @param out     数据输出流
     * @throws Exception
     */
    void encode(final T payload, final ByteBuf out) throws Exception;

    /**
     * 编码类型
     *
     * @return
     */
    short type();
}
