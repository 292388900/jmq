package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.RemoveConsumer;
import com.ipd.jmq.common.network.v3.command.Serializer;
import io.netty.buffer.ByteBuf;

/**
 * 移除消费者编码器
 */
public class RemoveConsumerEncoder implements PayloadEncoder<RemoveConsumer> {

    @Override
    public void encode(final RemoveConsumer payload, final ByteBuf out) throws Exception {
        payload.validate();
        Serializer.write(payload.getConsumerId().getConsumerId(), out);
    }

    @Override
    public int type() {
        return CmdTypes.REMOVE_CONSUMER;
    }
}