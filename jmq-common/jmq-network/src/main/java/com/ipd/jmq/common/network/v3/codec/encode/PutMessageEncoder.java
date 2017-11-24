package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.PutMessage;
import com.ipd.jmq.common.network.v3.command.Serializer;
import io.netty.buffer.ByteBuf;

/**
 * 发送消息编码器
 */
public class PutMessageEncoder implements PayloadEncoder<PutMessage> {

    @Override
    public void encode(final PutMessage payload, final ByteBuf out) throws Exception {
        payload.validate();
        // 1字节生产者ID长度
        Serializer.write(payload.getProducerId().getProducerId(), out);

        // 1字节事务ID长度

        if (payload.getTransactionId() != null) {
            Serializer.write(payload.getTransactionId().getTransactionId(), out);
        } else {
            Serializer.write((String) null, out);
        }
        // 消息体
        Serializer.write(payload.getMessages(), out);
        // 2字节队列ID
        out.writeShort(payload.getQueueId());
    }

    @Override
    public int type() {
        return CmdTypes.PUT_MESSAGE;
    }
}