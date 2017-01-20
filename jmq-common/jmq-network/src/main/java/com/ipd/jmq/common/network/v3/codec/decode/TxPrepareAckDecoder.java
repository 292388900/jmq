package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.v3.command.TxPrepareAck;
import com.ipd.jmq.common.network.v3.session.TransactionId;
import io.netty.buffer.ByteBuf;

/**
 * 事务准备响应解码器
 */
public class TxPrepareAckDecoder implements PayloadDecoder<TxPrepareAck> {

    @Override
    public TxPrepareAck decode(final TxPrepareAck payload, final ByteBuf in) throws Exception {
        payload.setTopic(Serializer.readString(in));
        String txId = Serializer.readString(in);
        payload.setTransactionId(txId == null || txId.isEmpty() ? null : new TransactionId(txId));
        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.TX_PREPARE_ACK;
    }
}