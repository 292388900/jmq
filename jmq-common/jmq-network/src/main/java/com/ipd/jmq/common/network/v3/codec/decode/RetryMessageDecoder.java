package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.RetryMessage;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.v3.session.ConsumerId;
import io.netty.buffer.ByteBuf;

/**
 * 重试消息解码器
 */
public class RetryMessageDecoder implements PayloadDecoder<RetryMessage> {

    public RetryMessage decode(final RetryMessage payload, final ByteBuf in) throws Exception {
        payload.setConsumerId(new ConsumerId(Serializer.readString(in)));
        payload.setException(Serializer.readString(in, 2, true));
        payload.setLocations(Serializer.readMessageLocations(in));
        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.RETRY_MESSAGE;
    }
}