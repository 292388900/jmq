package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.message.BrokerMessage;
import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.GetRetryAck;
import com.ipd.jmq.common.network.v3.command.Serializer;
import io.netty.buffer.ByteBuf;

/**
 * 获取重试应答编码器
 */
public class GetRetryAckDecoder implements PayloadDecoder<GetRetryAck> {

    @Override
    public GetRetryAck decode(final GetRetryAck payload, final ByteBuf in) throws Exception {
        int count = in.readShort();
        BrokerMessage[] messages = new BrokerMessage[count];
        for (int i = 0; i < count; i++) {
            messages[i] = Serializer.readBrokerMessage(in);
        }

        payload.setMessages(messages);
        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.GET_RETRY_ACK;
    }
}