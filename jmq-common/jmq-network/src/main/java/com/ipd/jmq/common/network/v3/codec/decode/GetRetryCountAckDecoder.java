package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.GetRetryCountAck;
import com.ipd.jmq.common.network.v3.command.Serializer;
import io.netty.buffer.ByteBuf;

/**
 * 获取重试数量应答解码器
 */
public class GetRetryCountAckDecoder implements PayloadDecoder<GetRetryCountAck> {
    @Override
    public GetRetryCountAck decode(final GetRetryCountAck payload, final ByteBuf in) throws Exception {
        payload.setTopic(Serializer.readString(in));
        payload.setApp(Serializer.readString(in));
        payload.setCount(in.readInt());
        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.GET_RETRY_COUNT_ACK;
    }
}