package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.GetRetryCount;
import com.ipd.jmq.common.network.v3.command.Serializer;
import io.netty.buffer.ByteBuf;

/**
 * 获取重试次数编码器
 */
public class GetRetryCountEncoder implements PayloadEncoder<GetRetryCount> {

    @Override
    public void encode(final GetRetryCount payload, final ByteBuf out) throws Exception {
        // 1字节主题长度
        Serializer.write(payload.getTopic(), out);
        // 1字节应用长度
        Serializer.write(payload.getApp(), out);

    }

    @Override
    public int type() {
        return CmdTypes.GET_RETRY_COUNT;
    }
}