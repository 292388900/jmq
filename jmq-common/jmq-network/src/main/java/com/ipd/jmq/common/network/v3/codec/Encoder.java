package com.ipd.jmq.common.network.v3.codec;

import com.ipd.jmq.common.network.TransportException;
import io.netty.buffer.ByteBuf;

/**
 * 对象编码
 * Created by hexiaofeng on 16-6-23.
 */
public interface Encoder {

    /**
     * 编码
     *
     * @param obj 对象
     * @param buf 输出流
     * @throws TransportException.CodecException
     */
    void encode(Object obj, ByteBuf buf) throws TransportException.CodecException;

}
