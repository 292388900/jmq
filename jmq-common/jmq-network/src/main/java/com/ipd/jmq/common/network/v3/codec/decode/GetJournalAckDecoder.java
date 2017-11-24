package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.GetJournalAck;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.toolkit.buffer.RByteBuffer;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

/**
 * 获取日志响应解码器
 */
public class GetJournalAckDecoder implements PayloadDecoder<GetJournalAck> {

    @Override
    public GetJournalAck decode(final GetJournalAck payload, final ByteBuf in) throws Exception {
        // 8字节偏移量
        payload.setOffset(in.readLong());
        payload.setInsync(in.readBoolean());
        // 8字节校验和
        payload.setChecksum(in.readLong());
        // 4字节长度
        ByteBuffer buffer = Serializer.readByteBuffer(in);
        if (buffer != null) {
            payload.setBuffer(new RByteBuffer(buffer, null));
        }
        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.GET_JOURNAL_ACK;
    }
}