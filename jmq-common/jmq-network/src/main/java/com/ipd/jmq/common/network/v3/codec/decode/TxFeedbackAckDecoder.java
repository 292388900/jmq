package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.v3.command.TxFeedbackAck;
import com.ipd.jmq.common.network.v3.command.TxStatus;
import com.ipd.jmq.common.network.v3.session.TransactionId;
import io.netty.buffer.ByteBuf;


/**
 * 事务补偿响应答解码器
 */
public class TxFeedbackAckDecoder implements PayloadDecoder<TxFeedbackAck> {

    @Override
    public TxFeedbackAck decode(TxFeedbackAck payload, ByteBuf in) throws Exception {
        if (in == null) {
            return null;
        }
        payload.setTopic(Serializer.readString(in));
        payload.setTxStatus(TxStatus.valueOf(in.readByte()));
        payload.setTxStartTime(in.readLong());
        String queryId = Serializer.readString(in);
        payload.setQueryId(queryId == null || queryId.isEmpty()? null : queryId);
        String txId = Serializer.readString(in);
        payload.setTransactionId(txId == null || txId.isEmpty()? null : new TransactionId(txId));

        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.TX_FEEDBACK_ACK;
    }
}