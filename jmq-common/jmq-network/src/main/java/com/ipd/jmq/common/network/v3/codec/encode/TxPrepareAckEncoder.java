package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.v3.command.TxPrepareAck;
import io.netty.buffer.ByteBuf;

/**
 * 事务准备响应编码器
 */
public class TxPrepareAckEncoder implements PayloadEncoder<TxPrepareAck> {

    @Override
    public void encode(final TxPrepareAck payload, final ByteBuf out) throws Exception {
        Serializer.write(payload.getTopic(), out);
        Serializer.write(payload.getTransactionId() == null ? null : payload.getTransactionId().getTransactionId(), out);
    }

    @Override
    public int type() {
        return CmdTypes.TX_PREPARE_ACK;
    }
}