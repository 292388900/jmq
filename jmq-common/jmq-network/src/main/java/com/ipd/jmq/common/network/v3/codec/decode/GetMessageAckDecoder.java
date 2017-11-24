package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.message.BrokerMessage;
import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.GetMessageAck;
import com.ipd.jmq.common.network.v3.command.Serializer;
import io.netty.buffer.ByteBuf;

/**
 * 消息应答解码器
 */
public class GetMessageAckDecoder implements PayloadDecoder<GetMessageAck> {

    @Override
    public GetMessageAck decode(final GetMessageAck payload, final ByteBuf in) throws Exception {
        if (in == null) {
            return null;
        }
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
        return CmdTypes.GET_MESSAGE_ACK;
    }
}