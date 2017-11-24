package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.GetOffset;
import io.netty.buffer.ByteBuf;

/**
 * 获取偏移量解码器
 */
public class GetOffsetDecoder implements PayloadDecoder<GetOffset> {

    @Override
    public GetOffset decode(final GetOffset payload, final ByteBuf in) throws Exception {
        payload.setOffset(in.readLong());
        payload.setOptimized(in.readByte() == 1);
        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.GET_OFFSET;
    }
}