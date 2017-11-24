package com.ipd.jmq.common.network.v3.codec.encode;

import com.ipd.jmq.common.network.v3.command.AddConsumer;
import com.ipd.jmq.common.network.v3.command.CmdTypes;
import com.ipd.jmq.common.network.v3.command.Serializer;
import io.netty.buffer.ByteBuf;

/**
 * 添加消费者编码器
 */
public class AddConsumerEncoder implements PayloadEncoder<AddConsumer> {

    @Override
    public void encode(AddConsumer payload, ByteBuf out) throws Exception {
        payload.validate();
        // 1字节消费者ID长度
        Serializer.write(payload.getConsumerId().getConsumerId(), out);
        // 1字节主题长度
        Serializer.write(payload.getTopic(), out);
        // 2字节选择器长度
        Serializer.write(payload.getSelector(), out, 2);
    }

    @Override
    public int type() {
        return CmdTypes.ADD_CONSUMER;
    }
}