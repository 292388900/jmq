package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.GetMessageAck;
import io.netty.buffer.ByteBuf;

/**
 * 消息应答编码器
 */
public class GetMessageAckEncoder implements PayloadEncoder<GetMessageAck> {

    @Override
    public void encode(final GetMessageAck payload, final ByteBuf out) throws Exception {
        /**
         * GetMessageAck implements BufAllocator
         * So we do nothing here
         */
    }

    @Override
    public int type() {
        return CmdTypes.GET_MESSAGE_ACK;
    }
}