package com.ipd.jmq.common.network.kafka.codec.decode;

import com.ipd.jmq.common.network.v3.command.Payload;
import io.netty.buffer.ByteBuf;

/**
 * Created by zhangkepeng on 16-8-2.
 */
public interface PayloadDecoder<T extends Payload> {

    /**
     * @param in      数据输入流
     * @return
     * @throws Exception
     */
    T decode(final T payload, final ByteBuf in) throws Exception;

    /**
     * 解码类型
     *
     * @return
     */
    short type();
}
