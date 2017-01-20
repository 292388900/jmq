package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.v3.command.TxCommit;
import com.ipd.jmq.common.network.v3.session.TransactionId;
import io.netty.buffer.ByteBuf;

/**
 * 分布式事务提交解码器
 */
public class TxCommitDecoder implements PayloadDecoder<TxCommit> {

    @Override
    public TxCommit decode(TxCommit payload, ByteBuf in) throws Exception {
        payload.setAttrs(Serializer.readMap(in));
        payload.setTopic(Serializer.readString(in));
        payload.setTransactionId(new TransactionId(Serializer.readString(in)));
        payload.setEndTime(in.readLong());
        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.TX_COMMIT;
    }
}