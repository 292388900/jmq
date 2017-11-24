package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.GetConsumeOffset;
import com.ipd.jmq.toolkit.io.Compressors;
import com.ipd.jmq.toolkit.io.Zlib;
import com.ipd.jmq.toolkit.lang.Charsets;
import io.netty.buffer.ByteBuf;

/**
 * 获取消费位置解码器
 */
public class GetConsumeOffsetDecoder implements PayloadDecoder<GetConsumeOffset> {

    @Override
    public GetConsumeOffset decode(final GetConsumeOffset payload, final ByteBuf in) throws Exception {
        // 4字节长度
        int len = in.readInt();
        if (len > 0) {
            byte[] data = new byte[len];
            in.readBytes(data);
            data = Compressors.decompress(data, 0, data.length, Zlib.INSTANCE);
            payload.setOffset(new String(data, Charsets.UTF_8));
        }
        if (in.isReadable()) {
            payload.setSlaveConsume(in.readBoolean());
        }

        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.GET_CONSUMER_OFFSET;
    }
}