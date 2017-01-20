package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.PutMessage;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.v3.session.ProducerId;
import com.ipd.jmq.common.network.v3.session.TransactionId;
import io.netty.buffer.ByteBuf;

/**
 * 发送消息解码器
 */
public class PutMessageDecoder implements PayloadDecoder<PutMessage>  {

    @Override
    public PutMessage decode(final PutMessage payload, final ByteBuf in) throws Exception {
        payload.setProducerId(new ProducerId(Serializer.readString(in)));
        String id = Serializer.readString(in);
        if (id != null && !id.isEmpty()) {
            payload.setTransactionId(new TransactionId(id));
        }

        // 需要解码为BrokerMessage类型
        payload.setMessages(Serializer.readMessages(in));
        // 队列ID
        payload.setQueueId(in.readShort());
        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.PUT_MESSAGE;
    }
}