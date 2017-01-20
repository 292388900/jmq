package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.GetJournalAck;
import io.netty.buffer.ByteBuf;

/**
 * 获取日志响应编码器
 */
public class GetJournalAckEncoder implements PayloadEncoder<GetJournalAck> {

    @Override
    public void encode(GetJournalAck payload, ByteBuf out) throws Exception {
        // implementation at GetJournalAck.encodeBody()
    }

    @Override
    public int type() {
        return CmdTypes.GET_JOURNAL_ACK;
    }
}