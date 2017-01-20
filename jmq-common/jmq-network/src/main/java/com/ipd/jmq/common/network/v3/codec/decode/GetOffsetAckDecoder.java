package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.GetOffsetAck;
import io.netty.buffer.ByteBuf;

/**
 * 获取偏移量应答解码器
 */
public class GetOffsetAckDecoder implements PayloadDecoder<GetOffsetAck> {

    @Override
    public GetOffsetAck decode(final GetOffsetAck payload, final ByteBuf in) throws Exception {
        // 8字节偏移量
        payload.setOffset(in.readLong());
        // 8字节最大偏移量
        payload.setMaxOffset(in.readLong());
        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.GET_OFFSET_ACK;
    }
}