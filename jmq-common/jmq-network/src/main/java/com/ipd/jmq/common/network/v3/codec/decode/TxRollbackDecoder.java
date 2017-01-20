package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.v3.command.TxRollback;
import com.ipd.jmq.common.network.v3.session.TransactionId;
import io.netty.buffer.ByteBuf;

/**
 * 分布式事务回滚解码器
 */
public class TxRollbackDecoder implements PayloadDecoder<TxRollback> {

    @Override
    public TxRollback decode(TxRollback payload, ByteBuf in) throws Exception {
        // 1字节长度事务ID
        payload.setTransactionId(new TransactionId(Serializer.readString(in)));
        // n 字节主题
        payload.setTopic(Serializer.readString(in));

        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.TX_ROLLBACK;
    }
}