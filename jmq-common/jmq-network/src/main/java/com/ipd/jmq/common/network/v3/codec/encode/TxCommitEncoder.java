package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.v3.command.TxCommit;
import io.netty.buffer.ByteBuf;

/**
 * 分布式事务提交编码器
 */
public class TxCommitEncoder implements PayloadEncoder<TxCommit> {

    @Override
    public void encode(final TxCommit payload, final ByteBuf out) throws Exception {
        payload.validate();

        Serializer.write(payload.getAttrs(), out);
        Serializer.write(payload.getTopic(), out);
        Serializer.write(payload.getTransactionId().getTransactionId(), out);
        out.writeLong(payload.getEndTime());
    }

    @Override
    public int type() {
        return CmdTypes.TX_COMMIT;
    }
}