package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.v3.command.TxRollback;
import io.netty.buffer.ByteBuf;

/**
 * 分布式事务回滚编码器
 */
public class TxRollbackEncoder implements PayloadEncoder<TxRollback> {

    @Override
    public void encode(final TxRollback payload, final ByteBuf out) throws Exception {
        payload.validate();
        Serializer.write(payload.getTransactionId().getTransactionId(), out);
        Serializer.write(payload.getTopic(), out);
    }

    @Override
    public int type() {
        return CmdTypes.TX_ROLLBACK;
    }
}