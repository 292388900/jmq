package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.GetRetryCountAck;
import com.ipd.jmq.common.network.v3.command.Serializer;
import io.netty.buffer.ByteBuf;

/**
 * 获取重试次数应答编码器
 */
public class GetRetryCountAckEncoder implements PayloadEncoder<GetRetryCountAck> {
    @Override
    public void encode(GetRetryCountAck payload, ByteBuf out) throws Exception {
        // 1字节主题长度
        Serializer.write(payload.getTopic(), out);
        // 1字节应用长度
        Serializer.write(payload.getApp(), out);
        // 4字节重试条数
        out.writeInt(payload.getCount());
    }

    @Override
    public int type() {
        return CmdTypes.GET_RETRY_COUNT_ACK;
    }
}