package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.message.BrokerMessage;
import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.PutRetry;
import com.ipd.jmq.common.network.v3.command.Serializer;
import io.netty.buffer.ByteBuf;

/**
 * 添加重试解码器
 */
public class PutRetryDecoder implements PayloadDecoder<PutRetry> {

    @Override
    public PutRetry decode(final PutRetry payload, final ByteBuf in) throws Exception {
        payload.setTopic(Serializer.readString(in));
        payload.setApp(Serializer.readString(in));
        payload.setException(Serializer.readString(in, 2, true));

        int count = in.readShort();
        BrokerMessage[] messages = new BrokerMessage[count];
        for (int i = 0; i < count; i++) {
            messages[i] = Serializer.readBrokerMessage(in);
        }

        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.PUT_RETRY;
    }
}