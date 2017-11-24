package com.ipd.jmq.common.network.v3.codec.decode;

import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.RemoveConsumer;
import com.ipd.jmq.common.network.v3.command.Serializer;
import com.ipd.jmq.common.network.v3.session.ConsumerId;
import io.netty.buffer.ByteBuf;

/**
 * 移除消费者解码器
 */
public class RemoveConsumerDecoder implements PayloadDecoder<RemoveConsumer> {

    @Override
    public RemoveConsumer decode(RemoveConsumer payload, ByteBuf in) throws Exception {
        // 1字节消费者ID长度
        payload.setConsumerId(new ConsumerId(Serializer.readString(in)));
        return payload;
    }

    @Override
    public int type() {
        return CmdTypes.REMOVE_CONSUMER;
    }
}