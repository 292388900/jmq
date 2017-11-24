package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.GetOffset;
import io.netty.buffer.ByteBuf;

/**
 * 获取偏移量编码器
 */
public class GetOffsetEncoder implements PayloadEncoder<GetOffset> {

    @Override
    public void encode(final GetOffset payload, final ByteBuf out) throws Exception {
        payload.validate();
        // 8字节偏移量
        out.writeLong(payload.getOffset());
        // 1字节优化偏移量
        out.writeByte(payload.isOptimized() ? 1 : 0);
    }

    @Override
    public int type() {
        return CmdTypes.GET_OFFSET;
    }
}