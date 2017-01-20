package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.v3.command.TxFeedback;
import io.netty.buffer.ByteBuf;

/**
 * 分布式事务补偿编码器
 */
public class TxFeedbackEncoder implements PayloadEncoder<TxFeedback> {

    @Override
    public void encode(final TxFeedback payload, final ByteBuf out) throws Exception {
        payload.validate();
        Serializer.write(payload.getTopic(), out);
        Serializer.write(payload.getApp(), out);
        out.writeByte(payload.getTxStatus().ordinal());
        out.writeInt(payload.getLongPull());
        Serializer.write(payload.getProducerId() != null ? payload.getProducerId().getProducerId() : null, out);
        Serializer.write(payload.getTransactionId() != null ? payload.getTransactionId().getTransactionId() : null, out);
    }

    @Override
    public int type() {
        return CmdTypes.TX_FEEDBACK;
    }
}