package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.RetryMessage;
import com.ipd.jmq.common.network.v3.command.Serializer;
import io.netty.buffer.ByteBuf;

/**
 * 重试消息编码器
 */
public class RetryMessageEncoder implements PayloadEncoder<RetryMessage> {

    public void encode(final RetryMessage payload, final ByteBuf out) throws Exception {
        payload.validate();
        // 1字节消费者ID长度
        Serializer.write(payload.getConsumerId().getConsumerId(), out);
        Serializer.write(payload.getException(), out, 2, true);
        Serializer.write(payload.getLocations(), out);
    }

    @Override
    public int type() {
        return CmdTypes.RETRY_MESSAGE;
    }
}