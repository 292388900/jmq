package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.message.BrokerMessage;
import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.GetRetryAck;
import com.ipd.jmq.common.network.v3.command.Serializer;
import io.netty.buffer.ByteBuf;

/**
 * 获取重试应答编码器
 */
public class GetRetryAckEncoder implements PayloadEncoder<GetRetryAck> {
    @Override
    public void encode(final GetRetryAck payload, final ByteBuf out) throws Exception {
        BrokerMessage[] messages = payload.getMessages();
        // 2字节条数
        if (messages == null) {
            out.writeShort(0);
            return;
        }
        out.writeShort(messages.length);

        for (BrokerMessage message : messages) {
            Serializer.write(message, out);
        }
    }

    @Override
    public int type() {
        return CmdTypes.GET_RETRY_ACK;
    }
}