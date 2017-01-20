package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.GetOffsetAck;
import io.netty.buffer.ByteBuf;

/**
 * 获取偏移量响应编码器
 */
public class GetOffsetAckEncoder implements PayloadEncoder<GetOffsetAck> {

    @Override
    public void encode(final GetOffsetAck payload, final ByteBuf out) throws Exception {
        payload.validate();
        out.writeLong(payload.getOffset());
        out.writeLong(payload.getMaxOffset());
    }

    @Override
    public int type() {
        return CmdTypes.GET_OFFSET_ACK;
    }
}