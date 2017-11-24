package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.GetJournal;
import io.netty.buffer.ByteBuf;

/**
 * 获取日志编码器
 */
public class GetJournalEncoder implements PayloadEncoder<GetJournal> {

    @Override
    public void encode(final GetJournal payload, final ByteBuf out) throws Exception {
        payload.validate();
        // 8字节偏移量
        out.writeLong(payload.getOffset());
        // 8字节最大偏移量
        out.writeLong(payload.getMaxOffset());
        // 4字节拉取等待时间
        out.writeInt(payload.getPullTimeout());
    }

    @Override
    public int type() {
        return CmdTypes.GET_JOURNAL;
    }
}