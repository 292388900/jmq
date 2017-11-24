package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.PutRetry;
import com.ipd.jmq.common.network.v3.command.Serializer;
import io.netty.buffer.ByteBuf;

/**
 * 添加重试编码器
 */
public class PutRetryEncoder implements PayloadEncoder<PutRetry> {

    @Override
    public void encode(final PutRetry payload, final ByteBuf out) throws Exception {
        payload.validate();
        // 1字节主题长度
        Serializer.write(payload.getTopic(), out);
        // 1字节应用长度
        Serializer.write(payload.getApp(), out);
        // 2字节长度异常信息
        Serializer.write(payload.getException(), out, 2, true);
        // 重试消息
        Serializer.write(payload.getMessages(), out);
    }

    @Override
    public int type() {
        return CmdTypes.PUT_RETRY;
    }
}