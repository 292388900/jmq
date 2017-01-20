package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.v3.command.TxFeedback;
import com.ipd.jmq.common.network.v3.command.TxStatus;
import com.ipd.jmq.common.network.v3.session.ProducerId;
import com.ipd.jmq.common.network.v3.session.TransactionId;
import io.netty.buffer.ByteBuf;

/**
 * 分布式事务补偿解码器
 */
public class TxFeedbackDecoder implements PayloadDecoder<TxFeedback> {

    @Override
    public TxFeedback decode(final TxFeedback payload, final ByteBuf in) throws Exception {
        payload.setTopic(Serializer.readString(in));
        payload.setApp(Serializer.readString(in));
        payload.setTxStatus(TxStatus.valueOf(in.readByte()));
        payload.setLongPull(in.readInt());
        String producerId = Serializer.readString(in);
        payload.setProducerId(producerId == null || producerId.isEmpty() ? null : new ProducerId(producerId));
        String txId = Serializer.readString(in);
        payload.setTransactionId(txId == null || txId.isEmpty() ? null : new TransactionId(txId));
        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.TX_FEEDBACK;
    }
}