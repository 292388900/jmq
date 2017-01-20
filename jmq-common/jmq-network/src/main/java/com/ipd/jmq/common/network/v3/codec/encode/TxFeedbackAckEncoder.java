package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.v3.command.TxFeedbackAck;
import com.ipd.jmq.common.network.v3.session.TransactionId;
import io.netty.buffer.ByteBuf;

/**
 * 分布式事务补偿应答编码器
 */
public class TxFeedbackAckEncoder implements PayloadEncoder<TxFeedbackAck> {

    @Override
    public void encode(TxFeedbackAck payload, ByteBuf out) throws Exception {
        if (out == null) {
            return;
        }
        payload.validate();
        Serializer.write(payload.getTopic(), out);
        out.writeByte(payload.getTxStatus().ordinal());
        out.writeLong(payload.getTxStartTime());
        String queryId = payload.getQueryId();
        Serializer.write(queryId != null && !queryId.isEmpty()? queryId : null, out);
        TransactionId transactionId = payload.getTransactionId();
        Serializer.write(transactionId != null? transactionId.getTransactionId() : null, out);
    }

    @Override
    public int type() {
        return CmdTypes.TX_FEEDBACK_ACK;
    }
}