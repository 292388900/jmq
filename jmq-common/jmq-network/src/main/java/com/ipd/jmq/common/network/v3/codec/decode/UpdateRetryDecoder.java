package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.v3.command.UpdateRetry;
import io.netty.buffer.ByteBuf;

/**
 * 更新重试解码器
 */
public class UpdateRetryDecoder implements PayloadDecoder<UpdateRetry> {

    @Override
    public UpdateRetry decode(final UpdateRetry payload, final ByteBuf in) throws Exception {
        payload.setTopic(Serializer.readString(in));
        payload.setApp(Serializer.readString(in));
        payload.setUpdateType(in.readByte());
        short count = in.readShort();
        if (count < 0) {
            count = 0;
        }
        long[] messages = new long[count];
        for (int i = 0; i < count; i++) {
            messages[i] = in.readLong();
        }
        payload.setMessages(messages);
        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.UPDATE_RETRY;
    }
}