package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.message.Message;
import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.v3.command.TxPrepare;
import io.netty.buffer.ByteBuf;

import java.util.List;

/**
 * 分布式事务主备编码器
 */
public class TxPrepareEncoder implements PayloadEncoder<TxPrepare> {

    @Override
    public void encode(final TxPrepare payload, final ByteBuf out) throws Exception {
        payload.validate();
        Serializer.write(payload.getAttrs(), out);
        Serializer.write(payload.getTransactionId() != null ? payload.getTransactionId().getTransactionId() : null, out);
        Serializer.write(payload.getProducerId() != null ? payload.getProducerId().getProducerId() : null, out);
        Serializer.write(payload.getTopic(), out);
        Serializer.write(payload.getQueryId(), out);
        out.writeLong(payload.getStartTime());
        out.writeInt(payload.getTimeout());
        out.writeByte(payload.getTimeoutAction());
        List<Message> messages = payload.getMessages();
        int count = messages == null ? 0 : messages.size();
        // 2字节消息个数
        out.writeShort(count);
        if (count > 0) {
            for (Message message : messages) {
                Serializer.write(message, out);
            }
        }
    }

    @Override
    public int type() {
        return CmdTypes.TX_PREPARE;
    }
}