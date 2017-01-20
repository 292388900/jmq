package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.GetConsumeOffset;
import com.ipd.jmq.toolkit.io.Compressors;
import com.ipd.jmq.toolkit.io.Zlib;
import com.ipd.jmq.toolkit.lang.Charsets;
import io.netty.buffer.ByteBuf;

/**
 * 获取消费位置编码器
 */
public class GetConsumeOffsetEncoder implements PayloadEncoder<GetConsumeOffset> {

    @Override
    public void encode(final GetConsumeOffset payload, final ByteBuf out) throws Exception {
        payload.validate();
        String offset = payload.getOffset();
        if (offset != null && !offset.isEmpty()) {
            byte[] data = payload.getOffset().getBytes(Charsets.UTF_8);
            data = Compressors.compress(data, 0, data.length, Zlib.INSTANCE);
            out.writeInt(data.length);
            out.writeBytes(data);
        } else {
            out.writeInt(0);
        }
        out.writeBoolean(payload.isSlaveConsume());
    }

    @Override
    public int type() {
        return CmdTypes.GET_CONSUMER_OFFSET;
    }
}