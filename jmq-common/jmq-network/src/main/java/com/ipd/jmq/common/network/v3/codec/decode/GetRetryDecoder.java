package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.GetRetry;
import com.ipd.jmq.common.network.v3.command.Serializer;
import io.netty.buffer.ByteBuf;

/**
 * 获取重试解码器
 */
public class GetRetryDecoder implements PayloadDecoder<GetRetry> {

    @Override
    public GetRetry decode(final GetRetry payload, final ByteBuf in) throws Exception {
        payload.setTopic(Serializer.readString(in));
        payload.setApp(Serializer.readString(in));
        payload.setCount(in.readShort());
        payload.setStartId(in.readLong());
        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.GET_RETRY;
    }
}