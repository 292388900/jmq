package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.GetRetry;
import com.ipd.jmq.common.network.v3.command.Serializer;
import io.netty.buffer.ByteBuf;

/**
 * 获取重试编码器
 */
public class GetRetryEncoder implements PayloadEncoder<GetRetry> {

    @Override
    public void encode(final GetRetry payload, final ByteBuf out) throws Exception {
        payload.validate();
        Serializer.write(payload.getTopic(), out);
        Serializer.write(payload.getApp(), out);
        out.writeShort(payload.getCount());
        out.writeLong(payload.getStartId());
    }

    @Override
    public int type() {
        return CmdTypes.GET_RETRY;
    }
}