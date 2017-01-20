package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.GetRetryCount;
import com.ipd.jmq.common.network.v3.command.Serializer;
import io.netty.buffer.ByteBuf;

/**
 * 获取重试次数解码器
 */
public class GetRetryCountDecoder implements PayloadDecoder<GetRetryCount> {

    @Override
    public GetRetryCount decode(final GetRetryCount payload, final ByteBuf in) throws Exception {
        payload.setTopic(Serializer.readString(in));
        payload.setApp(Serializer.readString(in));
        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.GET_RETRY_COUNT;
    }
}