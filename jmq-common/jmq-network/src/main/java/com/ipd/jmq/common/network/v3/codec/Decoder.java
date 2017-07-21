package com.ipd.jmq.common.network.v3.codec;

import com.ipd.jmq.common.network.TransportException;
import io.netty.buffer.ByteBuf;

/**
 * 对象解码
 * Created by hexiaofeng on 16-6-23.
 */
public interface Decoder {

    /**
     * 解码
     *
     * @param clazz 类名
     * @param buf   输入流
     * @return
     * @throws TransportException.CodecException
     */
    Object decode(Class clazz, ByteBuf buf) throws TransportException.CodecException;

}
