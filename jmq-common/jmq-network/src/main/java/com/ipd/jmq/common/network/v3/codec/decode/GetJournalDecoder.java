package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.GetJournal;
import io.netty.buffer.ByteBuf;

/**
 * 获取日志解码器
 */
public class GetJournalDecoder implements PayloadDecoder<GetJournal> {

    @Override
    public GetJournal decode(final GetJournal payload, final ByteBuf in) throws Exception {
        // 8字节偏移量
        payload.setOffset(in.readLong());
        // 8字节最大偏移量
        payload.setMaxOffset(in.readLong());
        // 4字节拉取等待时间
        if (in.isReadable()) {
            payload.setPullTimeout(in.readInt());
        }
        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.GET_JOURNAL;
    }
}